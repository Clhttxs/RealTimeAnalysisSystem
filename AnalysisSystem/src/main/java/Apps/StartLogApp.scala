package Apps

import Apps.LogDiversionApp.context
import Beans.StartLog
import CommonUtils.RedisUtil
import Constant.{PrefixConstant, StopGracefullyConstant, TopicConstant}
import RealTimeUtils.{DStreamUtil, DateParseUtil, StopGracefullyUtil}
import com.alibaba.fastjson.JSON
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.phoenix.spark.toProductRDDFunctions
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}
import redis.clients.jedis.Jedis

object StartLogApp extends BaseApp {
  override var topic: String = TopicConstant.STARTUP_LOG
  override var groupId: String = "startLog"
  override var appName: String = "StartLogApp"
  override var batchDuration: Int = 10
  override var stopName: String = StopGracefullyConstant.STOPSTARTLOG

  /**
   * 将从Kafka读取的数据封装成样例类
   */
  def parseBean(rdd: RDD[ConsumerRecord[String, String]]): RDD[StartLog] ={
    //解析中间件数据都已分区为单位
    rdd.mapPartitions(partition =>{
      partition.map(record =>{
        //json字符串的一级字段和样例类的属性名(可以比json的字段更多)需要一一对应
        val log: StartLog = JSON.parseObject(record.value(), classOf[StartLog])
        log.start_time=DateParseUtil.parseMillTsToDateTime(log.ts.toLong)
        log.start_date=DateParseUtil.parseMillTsToDate(log.ts.toLong)
        log.id=log.start_time+"_"+log.mid
        log
      })
    })
  }

  /**
   * 将当前批次的数据按照 (日期，mid) 分组，分组后，挑选每组中ts最小的那条
   * 什么操作:  1.分组聚合(reduceByKey 支持map端聚合)操作   多--->少(推荐)
   *          2.groupByKey、take(1)
   */
  def removeDuplicateBeanInCommonBatch(rdd: RDD[StartLog]):RDD[StartLog] ={
    //不使用mapPartitions,所有分区一起去重
    rdd.map(log =>((log.start_date,log.mid),log)).
      reduceByKey((log1,log2)=>{
        //返回时间戳小的
        if(log1.ts<log2.ts) log1
        else log2
      }).values
  }

  /**
   *读取redis，判断当前设备是否已经在今天记录过startlog，如果有，在当前批次汇总的startlog就没有必要记录
   * 设计: redis中存储的K-V
            要存储的是一天中已经记录过启动日志的 所有设备(多值，集合)
        K: 体现唯一性
              日期
        V： 类型
              Set(去重)
   */
  def removeDuplicateBeanFromHistoryBatch(rdd: RDD[StartLog]): RDD[StartLog] =
  {
    //分区,与Redis中缓存的数据对比
    rdd.mapPartitions(partition =>{
      val jedis: Jedis = RedisUtil.getJedis
      //如何redis中没有这条log,保留;否则丢弃
      val logs: Iterator[StartLog] =
        partition.filter(log => !jedis.sismember(PrefixConstant.dau_redis_Preffix + log.start_date, log.mid))
      jedis.close()
      //rdd是个弹性、不可变、可分区的数据集
      logs
    })
  }

  /**
   * 将每日首次启动的日志写入hbase中，saveToPhoenix()
   * 参数说明
         1.tableName:String写入的表名,这个表需要存在
         2.cols: Seq[String]：RDD中的Bean的属性按照顺序，应该写入指定表中的哪些列
         3.创建HBase使用的Configuration(固定写法)：HBaseConfiguration.create;先new Configuration，再读取hbase-site.xml和hbase-default.xml
         4.zkurl:连接的zk地址
   */
  def saveMidToPhoenix(rdd: RDD[StartLog]) = {
    //导入phoenix提供的一些隐式转换方法
    import org.apache.phoenix.spark._
    rdd.saveToPhoenix("RealTime2022_StartLog",
      Seq("ID","OPEN_AD_MS","OS","CH","IS_NEW","MID","OPEN_AD_ID","VC","AR", "UID","ENTRY","OPEN_AD_SKIP_MS","MD","LOADING_TIME","BA","TS","START_DATE","START_TIME"),
      HBaseConfiguration.create(),
      Some("hadoop102:2181:/hbase")
    )
  }

  def saveMidToRedis(rdd: RDD[StartLog]) = {
    /*只创建了一个连接，在数据量比较大的情况下，这样会导致连接超时或连接数不够用等问题
       val jedis = RedisUtil.getJedis()
     */
    rdd.foreachPartition(partition =>{
      val jedis = RedisUtil.getJedis()
      partition.foreach(log =>{
        jedis.sadd(PrefixConstant.dau_redis_Preffix+log.start_date,log.mid)
        //设置这个key24小时过期
        jedis.expire(PrefixConstant.dau_redis_Preffix+log.start_date, 24 * 60 * 60)
      })
      jedis.close()
    })
  }

  def main(args: Array[String]): Unit = {
    context = new StreamingContext("local[*]",appName,Seconds(batchDuration))
    runApp{
      val ds = DStreamUtil.createDStream(context, topic, groupId,"false","earliest")
      ds.map(_.value()).print()
      ds.foreachRDD(rdd =>{
        //1.获取偏移量
        val ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //2.自己的逻辑
        //封装样例类
        val startLog: RDD[StartLog] = parseBean(rdd)
        //同批次去重
        val log: RDD[StartLog] = removeDuplicateBeanInCommonBatch(startLog)
        //历史批次去重
        val log1 = removeDuplicateBeanFromHistoryBatch(log)
        //写入hbase和redis
        log1.cache()
        println("写入了"+log1.count()+"条数据！")
        saveMidToPhoenix(log1)
        //将已经记录过的log的mid写入redis
        saveMidToRedis(log1)
        //3.提交偏移量
        ds.asInstanceOf[CanCommitOffsets].commitAsync(ranges)
      })
    }
  }
}

