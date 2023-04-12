package Exactly_once

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
exactly one 精准一次消费：at least one+幂等性/事物
1.at least one+幂等性：下游支持幂等性操作，重复数据会覆盖写入：如hbase、redis、mysql
------------------------------------------------------------------------------------------
2.at least one+事物：下游支持事物操作，并且把offset存入到下游的数据库中(需要建一个表存放offset)
(1)查询之前写入数据库的偏移量Offsets
(2)从offsets位置获取一个流
(3)自己的运算，得到结果
(4)在一个事务中把结果和偏移量写出到数据库
以wordcount(累加)为例
设计mysql中怎么存储数据，offsets
数据:
   粒度:个word是一行
   主键: word
offset: groupId, topic , partitionid, offset
  粒度:一个组消费一个主题的一个分区是一行
  主键: (groupId, topic, partitionid)
*/

object ExactlyOnceTransaction {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("loacl[*]").setAppName("exactlyone")
    val ssc = new StreamingContext(conf, Seconds(5))
    val kafkapara = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "exactly",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value,deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )
    val kafkads = ConsumerStrategies.Subscribe[String, String](Set("exactly_one"), kafkapara)
    KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      kafkads
    )
//    1.查询之前写入数据库的偏移量Offsets

  }
}
