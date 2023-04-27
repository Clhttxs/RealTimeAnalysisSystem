package Apps

import Apps.StartLogApp.context
import CommonUtils.RedisUtil
import Constant.StopGracefullyConstant
import RealTimeUtils.StopGracefullyUtil
import org.apache.spark.streaming.{StreamingContext, StreamingContextState}

/**
 * BaseApp:把编写一个SparkStreamingApp的一般步骤抽象出来。
 * 不同的业务类，只需要集成BaseApp，就可以拥有编程的一般步骤，无需重复编写
 * StreamingContext包含:masterurl、appname、batchDuration
 *
 */
//abstract关键字修饰抽象类
abstract class BaseApp {
  /**
   * 声明抽象属性，不需要赋值
   * 只有抽象类和特质才能声明抽象属性
   * scala抽象属性必须用var修饰
   */
  var topic:String
  var groupId:String
  var appName:String
  var batchDuration:Int
  var stopName:String
  //由子类overwrite，子类根据自己的appName和batchDuration去创建自己的StreamingContext
  var context:StreamingContext=null
  //控制抽象,code是不同任务的具体的代码块
  def runApp(code: =>Unit): Unit ={
    try {
      code
      // 开启一个守护线程，每隔 10 秒检查一次 StreamingContext 的状态，如果已经停止则调用stop方法停止程序
      new Thread(() => {
        val jedis = RedisUtil.getJedis()
        var isStop: Boolean = true
        while (isStop) {
          if (StopGracefullyUtil.isStopGracefully(jedis,stopName)) {
            try {
              println("正在停止！")
              StopGracefullyUtil.rollBack(jedis,stopName)
              val state: StreamingContextState = context.getState
              if(state==StreamingContextState.ACTIVE) {
                println("即将优雅的停止")
                context.stop(stopSparkContext = true, stopGracefully = true)
                println("优雅的停止完毕")
                context.sparkContext.stop()
                println("Spark context已经关闭")
                isStop=false
                System.exit(0)
              }
            } catch {
              case e:Exception => {

                println("优雅的关闭出错！")
              }
            }
          } else {
            Thread.sleep(1000)
          }
        }
      }).start()
      context.start()
      context.awaitTermination()
    } catch {
      case e:Exception =>{
        e.printStackTrace()
        throw new RuntimeException("运行出错！")
      }
    }
  }
}
