package Apps

import org.apache.spark.streaming.StreamingContext

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
  //由子类overwrite，子类根据自己的appName和batchDuration去创建自己的StreamingContext
  var context:StreamingContext = null
  //控制抽象,code是不同任务的具体的代码块
  def runApp(code: =>Unit): Unit ={
    try {
      code
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
