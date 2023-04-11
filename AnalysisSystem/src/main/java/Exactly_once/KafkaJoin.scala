package Exactly_once

import org.apache.kafka.clients.consumer
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.awt.RenderingHints.Key

object KafkaJoin {
  def main(args: Array[String]): Unit = {
    //创建sparkstreaming的入口
    val conf = new SparkConf().setMaster("local[*]").setAppName("ExactlyOne")
    //时间设置长一点,方便观察测试效果
    val ssc = new StreamingContext(conf, Seconds(30))
    //定义Kafka的连接参数
    val kafkaPare = Map[String,Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "clh",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )
    val kafkaPare1 = Map[String,Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "test",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )
    //利用kafka工具类读取Kafka数据，创建DStream
    val eds: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent, //位置策略
      ConsumerStrategies.Subscribe[String, String](Set("exactly_one"), kafkaPare) //主题和配置参数
    )
    val fds = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("first"), kafkaPare1)
    )
    //取出每条消息
//    val dsv = eds.map(record => record.value());
//    dsv.print()
    //对kafkardd进行join操作，需要先转换成kv类型的rdd
    //我顺便做了一个wordcount
    val ds1: DStream[(String, Int)] = eds.map(_.value()).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    val ds2: DStream[(String, Int)] = fds.map(_.value()).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    ds1.leftOuterJoin(ds2).print()
    println()
    ds1.fullOuterJoin(ds2).print()
    println()
    ds1.rightOuterJoin(ds2).print()
    println()
    //内连接
    ds1.join(ds2).print()
    ssc.start()
    ssc.awaitTermination();
  }
}
