import Apps.BaseApp
import Apps.GMVApp.{appName, batchDuration, context, groupId, topic}
import CommonUtils.KafkaClientUtil
import Constant.{StopGracefullyConstant, TopicConstant}
import RealTimeUtils.{DStreamUtil, DateParseUtil}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TestApp extends BaseApp{
  override var topic: String = "test230514"
  override var groupId: String = "Test230514"
  override var appName: String = "AppTest"
  override var batchDuration: Int = 10
  override var stopName: String = StopGracefullyConstant.STOPGVM
  def main(args: Array[String]): Unit = {
    context = new StreamingContext("local[*]",appName,Seconds(batchDuration))
    runApp{
      val ds = DStreamUtil.createDStream(context, topic, groupId,"true","earliest")
      val ds1 = ds.map(_.value())
      ds1.print()
      ds1.foreachRDD(rdd =>{
        println("rdd的元素数量为"+rdd.count)
        rdd.mapPartitions(partition =>{
          println("rdd分区的元素数量为"+partition.size)
          partition
        })
      })

    }
  }


}
