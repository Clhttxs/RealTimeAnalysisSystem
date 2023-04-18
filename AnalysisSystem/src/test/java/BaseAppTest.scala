import Apps.BaseApp
import RealTimeUtils.DStreamUtil
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BaseAppTest extends BaseApp {
  override var topic: String = "exactly_one"
  override var groupId: String = "baseApp"
  override var appName: String = "baseAppTest"
  override var batchDuration: Int = 10

  def main(args: Array[String]): Unit = {
    //重写context
    context = new StreamingContext("local[*]",appName,Seconds(batchDuration))
    //继承runApp方法
    runApp {
      val ds = DStreamUtil.createDStream(context, topic, groupId)
      ds.map(_.value()).print()
    }
  }
}
