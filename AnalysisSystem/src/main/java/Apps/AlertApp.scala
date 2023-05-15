package Apps

import Beans.{ActionLog, CouponAlertInfo}
import CommonUtils.PropertiesUtil
import Constant.{StopGracefullyConstant, TopicConstant}
import RealTimeUtils.{DStreamUtil, DateParseUtil}
import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.omg.CORBA.Current

import java.time.LocalDate
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

/**
 * es具有幂等性,因此使用at least once + 幂等性,保证精确一次性消费
 */
object AlertApp extends BaseApp {
  override var topic: String = TopicConstant.ACTION_LOG
  override var groupId: String = "AlertLog"
  override var appName: String = "AlertApp"
  override var batchDuration: Int = 10
  override var stopName: String = StopGracefullyConstant.StopAlert

  def main(args: Array[String]): Unit = {
    //设置ES的相关参数
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(appName)
    //采用ES进行存储，需要在SparkConf中配置相关参数
    //配置ES的集群地址和端口号
    sparkConf.set("es.nodes",PropertiesUtil.getProperty("es.nodes"))
    sparkConf.set("es.port",PropertiesUtil.getProperty("es.port"))
    //允许自动创建索引
    sparkConf.set("es.index.auto.create", "true")
    //允许将主机名转化成ip
    sparkConf.set("es.nodes.wan.only", "true")
    context = new StreamingContext(sparkConf,Seconds(batchDuration))
    runApp{
      val ds = DStreamUtil.createDStream(context, topic, groupId)
      var ranges: Array[OffsetRange]=null
      /**
       * 使用transform获取偏移量,返回一个新的rdd。然后在对返回的rdd进行开窗(5分钟)
       * 不能使用foreach,因为foreach是输出算子，没有返回值。无法对ds进行开窗
       */
      val ds1 = ds.transform(rdd => {
        ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.mapPartitions(partition => {
          partition.map(record => {
            JSON.parseObject(record.value(), classOf[ActionLog])
          })
        })
      })

      /**采集过去5min的数据
       * 对ds1进行开窗window(窗口大小，步长)：窗口大小和步长都得是批次周期的整数倍
       * 窗口大小等于步长：数据刚好全部被计算
       * 窗口大小大于步长：存在重复计算
       * 窗口大小小于步长：会导致漏运算
       */
      val ds2 = ds1.window(Minutes(5), Seconds(30))//步长30s为了看到测试效果
      //ds2.print()
      //按照用户和设备进行分组，把每个用户在过去5min在每个设备上的所有动作汇总
      val ds3: DStream[((String, String), Iterable[ActionLog])] = ds2.map(log => ((log.mid, log.uid), log)).groupByKey()
      //过滤判断用户是否增加了收货地址
      val ds4 = ds3.filter {
        case ((mid, uid), logs) => {
          var ifNeedAlert: Boolean = false
          Breaks.breakable {
            for (log <- logs) {
              if ("trade_add_address".equals(log.action_id)) {
                //用户指定了增加收货地址的操作，有嫌疑
                ifNeedAlert = true
                //无需再继续判断，直接退出循环
                Breaks.break()
              }
            }
          }
          ifNeedAlert
        }
      }
      ds4.foreachRDD(rdd =>{
        println("新增收获地址的用户有"+rdd.count())
      })
      //判断每个设备上增加了收货地址的人数是否>=2,如果是就留下
      //ds4.map(alertLog => (alertLog._1._1,alertLog._2)).groupByKey()
      val ds5: DStream[(String, Iterable[Iterable[ActionLog]])] = ds4.map {
        case (((mid, uid), log)) => {
          (mid, log)
        }
      }.groupByKey()
      //符合预警条件(同一设备5分钟内,有2个以上的用户修改过地址)的设备及用户在设备上的预警日志
      val ds6: DStream[(String, Iterable[Iterable[ActionLog]])] = ds5.filter(_._2.size >= 2)
      //将数据扁平化
      val ds7: DStream[(String, Iterable[ActionLog])] = ds6.mapValues(_.flatten)
      ds7.foreachRDD(rdd =>{
        println("需要预警的设备有"+rdd.count())
      })

      //生成预警日志,封装成样例类
      val ds8: DStream[CouponAlertInfo] = ds7.map {
        case (mid, logs) => {
          val uids: mutable.Set[String] = new mutable.HashSet[String]
          val itemIds: mutable.Set[String] = new mutable.HashSet[String]
          val events: ListBuffer[String] = new ListBuffer[String]
          //封装预警信息
          logs.foreach(log => {
            uids.add(log.uid)
            events.append(log.action_id)
            if ("favor_add".equals(log.action_id)) {
              itemIds.add(log.item)
            }
          })
          //添加ts
          val ts: Long = System.currentTimeMillis()
          /*
              添加id(体现mid)
              同一设备，如果一分钟产生多条预警，只保留最后一条预警日志
              同一分钟，一个设备的预警日志的id要相同，后续产生的才会覆盖之前产生的。
              PUT /index/type/id   {log1}
              PUT /index/type/id   {log2}
              id:mid_分钟
           */
          val minStr: String = DateParseUtil.parseMillTsToDateMinute(ts)
          val id = minStr + "_" + mid
          CouponAlertInfo(id, uids, itemIds, events, ts)
        }
      }
      //写入Es
      import org.elasticsearch.spark._
      ds8.foreachRDD(rdd =>{
        rdd.cache()
        println("当前要写入ES:"+rdd.count())
        /**
         * *resource:String: 要写入的index/type
         * cfg:Map[String,String]:要写入的index的配置说明
               es.mapping.id:当前RDD中的T类型的哪个属性是作为主键
         */
        rdd.saveToEs("realtime2022_behaviour_alert_"+LocalDate.now(),Map("es.mapping.id"->"id"))
        //提交偏移量
        ds.asInstanceOf[CanCommitOffsets].commitAsync(ranges)
      })
    }
  }
  /**
   * 这个程序存在的bug:
   * 1.窗口固定大小和步长，如何一个设备满足预警条件，但是被划分到了两个窗口，则不会触发预警
   * 解决办法：
   * (1)使用有状态函数，updateStateByKey,将rdd历史状态控制在五分钟。
   *    使用updateStateByKey实现实时窗口计算，实现了保留5分钟历史记录的实时窗口计算：
    val historyDStream = inputStream
        .map(data => (data.key, data.value))
        .updateStateByKey[Int](updateFunc)
        .filter(_._2.isDefined)
        .mapValues(_.get)
        .window(windowSize, slideInterval)
     windowSize和slideInterval则分别表示窗口大小和滑动间隔，这里设置为5分钟和1分钟，可以根据需要进行调整。
   *(2)增大窗口大小和减少步长：列如窗口大小10分钟，5分钟提交一次。但是这样会导致重复计算，导致效率降低。
   *(3)使用flink,spark是微批次，准实时。在实时处理这方便还是比flink差
   * ——————————————————————————————————————————————————————————————————————————————————————————————————————————————————
   * 2.数据漂移问题,23:59:50产生的数据跑到第二天的index里面去了
   * 解决办法：
   * (1)只用一个index，不按天(分钟)使用index
   * (2)不使用spark的saveToEs算子,自己编写API写入es里面
   */
}
