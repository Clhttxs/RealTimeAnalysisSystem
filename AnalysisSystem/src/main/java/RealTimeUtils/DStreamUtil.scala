package RealTimeUtils

import CommonUtils.PropertiesUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object DStreamUtil {
  /**
   *获取一个Kafka的输入流
   * @param context 一个StreamingContext
   * @param topic 需要访问的主题
   * @param groupId 消费者组id
   * @param offsetReset 第一次从哪个位置消费，默认从latest。可选latest、earliest、none
   * @param isSaveToMysql 是否把偏移量保存到Mysql ，默认是false，不保存
   * @param offsets 从MySQL中获取的分区号和偏移量，默认是null，只有isSaveToMysql=true时，才需要穿传递
   * @return 返回一个Kafka的输入流
   */
  def createDStream(context:StreamingContext,topic:String,groupId:String,enableCommit:String = "false",offsetReset:String="latest",isSaveToMysql:Boolean=false,offsets:Map[TopicPartition, Long]=null): InputDStream[ConsumerRecord[String, String]] ={
    try {
      val kafkapara = Map[String, Object](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropertiesUtil.getProperty("kafka.broker.list"),
        ConsumerConfig.GROUP_ID_CONFIG -> groupId,
        "key.deserializer" -> PropertiesUtil.getProperty("key.deserializer"),
        "value.deserializer" -> PropertiesUtil.getProperty("value.deserializer"),
        //这个参数只影响第一次消费,第一次消费没有指定offsets，才读取此参数。
        //如果提交过偏移量，就会从提交的位置往后消费
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> offsetReset,
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> enableCommit
      )
      var kafkaDS: InputDStream[ConsumerRecord[String, String]] = null
      if (isSaveToMysql) {
        kafkaDS = KafkaUtils.createDirectStream[String, String](
          context,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Subscribe[String, String](Set(topic), kafkapara, offsets)
        )
      } else {
        kafkaDS = KafkaUtils.createDirectStream[String, String](
          context,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Subscribe[String, String](Set(topic), kafkapara)
        )
      }
      kafkaDS
    } catch {
      case e:Exception => {
        print("kafkaDS获取失败！")
        null
      }
    }
  }

}
