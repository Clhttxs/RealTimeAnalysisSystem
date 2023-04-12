package Exactly_once

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
1.at most one至少一次消费：自动提交offset
"enable.auto.commit" -> "true"
"auto_commit_interval_ms" -> "5"(ms)
"auto.offset.reset" -> "earliest|latest|none"
2.at least one 至少消费一次：
（1）手动提交offset
"enable.auto.commit" -> "false"
（2）获取偏移量，手动提交
 1.ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
 2.ds.asInstanceOf[CanCommitOffsets].commitAsync(ranges)
 ----------------------------------------------------------------------
  stream.foreachRDD { rdd =>
      // 在这里处理接收到的数据，将处理结果写入外部存储系统
      // ...

      // 手动提交 offset
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }
-------------------------------------------
 ds必须时初始的KafkaRDD
*/
object AtLeastOne {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("atleastone")
    val ssc = new StreamingContext(conf, Seconds(10))
    val kafkapara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "leastone",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "enable.auto.commit" -> "false"
    )
    val kafkads: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String]( //必须设置泛型
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("exactly_one"), kafkapara) //这个泛型需要与kafka流的泛型一致
    )
    //初始化一个数组,用于更新偏移量
    var ranges: Array[OffsetRange] = null
    //获取偏移量
    val ds1 = kafkads.transform( //transform和foreachRDD ：只有RDD.算子(xxx ) xxx是在Executor端运行，其他的位置都是在Driver端!
      rdd => {
        //driver执行,每采集一次执行一次
        println("aaaa:"+Thread.currentThread().getName)
        ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges//不是rdd算子,这个命令在driver中执行. 是对rdd本身操作,不是对rdd的数据进行操作.
        rdd
      }
    )
    val ds2 = ds1.map(_.value()).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    ds2.print()//action算子,这是一个job了，提交到executor上执行
    kafkads.foreachRDD(//必须用原始的KafkaRDD,这样才能进行类型转换
      rdd =>{
        println("bbbb:"+Thread.currentThread().getName)
        kafkads.asInstanceOf[CanCommitOffsets].commitAsync(ranges)//在executor端执行的,对数据进行操作.
      }
    )
    println("cccc:"+Thread.currentThread().getName)
    //kafkads.asInstanceOf[CanCommitOffsets].commitAsync(ranges) 直接在driver端执行(只会执行一次,生成job一个模板,并不会真正执行任务)会报空指针异常,ranges数组还没有初始化
    ssc.start()
    ssc.awaitTermination()
  }
}
