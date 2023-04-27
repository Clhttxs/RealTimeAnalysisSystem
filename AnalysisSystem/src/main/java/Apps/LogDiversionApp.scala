package Apps

import Apps.StartLogApp.context
import CommonUtils.{KafkaClientUtil, RedisUtil}
import Constant.{StopGracefullyConstant, TopicConstant}
import RealTimeUtils.{DStreamUtil, StopGracefullyUtil}
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.google.gson.Gson
import org.apache.kafka.clients.KafkaClient
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

import java.util

/**
 * 从kafka的base_log中读取数据，调出其中的 start_log和actions_log(炸裂，拆分)，将数据写回kafka.
 * 如果需要其他类型的log，可以在LogDiversionApp中扩展
 * 类似离线数仓:ods_log_inc输入-----hql --------挑选page_log ---------写入dwd_traffic_page_view_inc
 * 离线数仓的HDFS对应实时项目的kafka
 * 精准一次消费：at least once + 幂等输出(kafka prodecer开启幂等输出)
 */

/**
 * 1.fastjson:快。处理1088w条json，耗时短。不稳定
 * jsonstr:{}:json对象 单个;{}:json数组，多个
 * (1)对象转jsonstr:JSON .toJsoNString(对象|集合)
 * (2)jsonstr转对象:JSON.parseXxx()
 *    {}转单个对象:JSON.parse0bject("{}",classof[xxx]);
 *    []转集合对象:JSON .parseArray("[]",classof[xxx]);
 *    不写classof[xxx]默认转化程json对象
 * --------------------------------------------------------------------
 * 2.gson: google json:国外都用。用的是国外的框架，内置!
 * 全是实例方法
 * --------------------------------------------------------------------
 * 3.省流版:
 * scala样例类对象转jsonstr:new Gson( ).toJson(x)
 * Jsonstr转对象:JSON .parseXxx()
 */
object LogDiversionApp extends BaseApp {
  override var topic: String =TopicConstant.ORIGINAL_LOG
  override var groupId: String = "realtime01"
  override var appName: String = "LogDiversionApp"
  override var batchDuration: Int = 10
  override var stopName: String = StopGracefullyConstant.STOPLOGDIVERSION
  /**
   * 炸裂actions,并将每一条action、page、common聚合
   * @param jsonObject 整条日志log的
   * @param commonMap  已经聚合page的common
   * @param gson 一个gson对象
   * @param ACTION_LOG  主题名
   */
  def flatActions(jsonObject: JSONObject, commonMap: util.Map[String, AnyRef], gson: Gson, ACTION_LOG: String) = {
    val actionStrs = jsonObject.getString("actions")
    val array: JSONArray = JSON.parseArray(actionStrs)
    for(i <- 0 until array.size()){
      val actionMap = JSON.parseObject(array.getString(i)).getInnerMap
      //map合并,有重名的key,后面会覆盖前面
      commonMap.putAll(actionMap)
      KafkaClientUtil.sendDataToKafka(ACTION_LOG,gson.toJson(commonMap))
    }
  }

  /**
   *处理rdd，将start_log和active_log发送到Kafka对应发分区
   * @param rdd Kafka中base_log主题中要处理的rdd
   */
  def handleLog(rdd: RDD[ConsumerRecord[String, String]]) = {
    //去写数据库(中间件),都需要以分区为单位操作
    rdd.foreachPartition(partition =>{
      //每个分区创建一个Gson对象
      val gson = new Gson()
      partition.foreach(record =>{
        //取出Kafka里面的value
        //返回一个JSON对象,可以使用键值对的方式访问其中的属性值
        val jsonObject: JSONObject = JSON.parseObject(record.value())
        //只保留common部分
        val commonMap: util.Map[String, AnyRef] = JSON.parseObject(jsonObject.getString("common")).getInnerMap
        //判断日志类型
        if(jsonObject.containsKey("start") && !jsonObject.containsKey("err")){
          //封装start部分
          /**
           * getString：从一个JSON对象中获取名为 "start" 的字符串属性值
           * startStr的数据，json字符串
           * {
           * "entry": "icon",
           * "loading_time": 18776,
           * "open_ad_id": 5,
           * "open_ad_ms": 1506,
           * "open_ad_skip_ms": 0
           * }
           */
          val startStr: String = jsonObject.getString("start")
          /**getInnerMap返回一个Map对象，其中包含JSON对象的所有属性和值
           * AnyRef是Scala中所有引用类型的基类,它相当于Java中的Object类
           * startmap的数据：
           *{entry=icon, loading_time=18776, open_ad_id=5, open_ad_ms=1506, open_ad_skip_ms=0}
           */
          val startMap: util.Map[String, AnyRef] = JSON.parseObject(startStr).getInnerMap
          //添加时间戳,action不用这个时间戳,用action里面的ts
          commonMap.put("ts",jsonObject.getLong("ts"))
          //将startMap的K-V合并到resultMap中
          commonMap.putAll(startMap)
          //生产数据到kafka
          KafkaClientUtil.sendDataToKafka(TopicConstant.STARTUP_LOG,gson.toJson(commonMap))
        }else if(jsonObject.containsKey("actions")&& !jsonObject.containsKey("err")){
          //获取pageMap
          val pageMap = JSON.parseObject(jsonObject.getString("page")).getInnerMap
          commonMap.putAll(pageMap)
          //炸裂actions
          flatActions(jsonObject,commonMap,gson,TopicConstant.ACTION_LOG)
        }
      })
    })
    //整个rdd写完后刷写缓冲区
    KafkaClientUtil.flush()
  }

  def main(args: Array[String]): Unit = {
    context = new StreamingContext("local[*]",appName,Seconds(batchDuration))
    runApp{
      //获取输入流
      val kafkaDs = DStreamUtil.createDStream(context, topic, groupId)
      kafkaDs.map(_.value()).print()
      //对消费到的每条日志进行判断，再重新写入到Kafka对应的主题中区
      kafkaDs.foreachRDD(rdd =>{
        if(!rdd.isEmpty()){
          //获取偏移量
          val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          //处理日志
          handleLog(rdd)
          //提交偏移量
          kafkaDs.asInstanceOf[CanCommitOffsets].commitAsync(ranges)
        }
      })
    }
  }
}
