import CommonUtils.{KafkaClientUtil, RedisUtil}
import Constant.StopGracefullyConstant
import RealTimeUtils.{DStreamUtil, StopGracefullyUtil}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, LocationStrategies}

object ThreadTest {
  import org.apache.spark.SparkConf
  import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}
  import org.apache.spark.streaming.kafka010.KafkaUtils


    def main(args: Array[String]): Unit = {
      println("main开始")
      /**
       * 第一个线程使用了 lambda 表达式的方式创建了一个匿名函数，然后将其传递给了 Thread 类的构造函数中。这种方式可以在代码中直接使用表达式，简洁明了，可以避免使用过多的类和方法。
       */
     /* new Thread(()=>{
        while (true){
          println("--------------")
        }
      }).start()*/

      /**
       * 第二个线程则是通过实现 Runnable 接口来创建的。需要实现 run 方法，并将其作为参数传递给 Thread 的构造函数。
       * 这种方式可以让类更加灵活，因为一个类可以实现多个接口，可以实现多个 run 方法，从而让线程具有不同的行为。同时也可以避免由于单继承机制导致的限制。
       */
      new Thread(
        new Runnable {
          override def run(): Unit = {
            val jedis = RedisUtil.getJedis
            while (true){
             if(StopGracefullyUtil.isStopGracefully(jedis,StopGracefullyConstant.STOPSTARTLOG)){
               println("停止运行！")
               System.exit(0)
             }else{
               println("运行中")
               Thread.sleep(5000)
             }
            }
          }
        }
      ).start()
      println("main结束")
    }
}
