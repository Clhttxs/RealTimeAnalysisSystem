package Apps

import Apps.StartLogApp.{appName, batchDuration, context, groupId, topic}
import CommonUtils.RedisUtil
import Constant.{StopGracefullyConstant, TopicConstant}
import RealTimeUtils.{DStreamUtil, StopGracefullyUtil}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object TestApp extends BaseApp {
  override var topic: String = "test"
  override var groupId: String = "testapp"
  override var appName: String = "TestApp"
  override var batchDuration: Int = 10
  override var stopName: String = ""

  def main(args: Array[String]): Unit = {
    context = new StreamingContext("local[*]",appName,Seconds(batchDuration))
    runApp{
      val ds = DStreamUtil.createDStream(context, topic, groupId,"true","earliest")
      ds.map(_.value()).print()
      new Thread(() => {
        try {
          val jedis = RedisUtil.getJedis()
          while (true) {
            if (StopGracefullyUtil.isStopGracefully(jedis, StopGracefullyConstant.STOPSTARTLOG)) {
              println("正在停止！")
              println("11111111111111111111")
              println(s"当前 StreamingContext 状态为：${context.getState()}")
              context.stop(stopSparkContext = true, stopGracefully = true) //设置优雅的关闭
              println("优雅的停止完毕")
              StopGracefullyUtil.rollBack(jedis, StopGracefullyConstant.STOPSTARTLOG)
              jedis.close()
              context.awaitTermination() // 等待 StreamingContext 关闭完成
              context.sparkContext.stop()
              println("Spark context已经关闭")
              System.exit(0)
            } else {
              Thread.sleep(1000)
            }
          }
        } catch {
          case e: Exception =>
            println(s"线程发生异常，错误信息为：${e.getMessage}")
            e.printStackTrace()
        }
      }).start()
    }
  }
}
