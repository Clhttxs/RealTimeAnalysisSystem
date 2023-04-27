package RealTimeUtils

import CommonUtils.RedisUtil
import Constant.StopGracefullyConstant
import redis.clients.jedis.Jedis

/**
 * 这个工具类是用来优雅的停止streaming的，
 * 基本原理:
 * 1.这个工具类定义方法改变redis中数据;
 * 2.streaming读取redis中的对应的数据(哈希类型),检测到变化成true就优雅的停止
 */
object StopGracefullyUtil {

  //改变redis的数据,从而优雅的关闭streaming
  def stopGracefully(streamingName:String):Unit = {
    val jedis = RedisUtil.getJedis
    jedis.hset(StopGracefullyConstant.STOPGRACEFULLY,streamingName,"true")
    jedis.close()
  }

  //判断是否关闭streaming
  def isStopGracefully(jedis:Jedis,streamingName:String):Boolean={
    var flag:Boolean = false
    if(jedis.hget(StopGracefullyConstant.STOPGRACEFULLY,streamingName).equals("true")) flag=true;
    flag
  }

  //关闭streaming,重新初始化redis的数据
  def rollBack(jedis:Jedis,streamingName:String):Unit={
    jedis.hset(StopGracefullyConstant.STOPGRACEFULLY,streamingName,"false")
    println("rollback停止成功！")
  }
}
