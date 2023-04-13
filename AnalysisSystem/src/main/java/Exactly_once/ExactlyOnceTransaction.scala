package Exactly_once

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.JDBCUtil

import java.sql.{Connection, PreparedStatement, ResultSet}
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

/**
exactly one 精准一次消费：at least one+幂等性/事物
1.at least one+幂等性：下游支持幂等性操作，重复数据会覆盖写入：如hbase、redis、mysql
------------------------------------------------------------------------------------------
2.at least one+事物：下游支持事物操作，并且把offset存入到下游的数据库中(需要建一个表存放offset)
(1)查询之前写入数据库的偏移量Offsets
(2)从offsets位置获取一个流
(3)自己的运算，得到结果
(4)在一个事务中把结果和偏移量写出到数据库
以wordcount(累加)为例
设计mysql中怎么存储数据，offsets
数据:
   粒度:个word是一行
   主键: word
offset: groupId, topic , partitionid, offset
  粒度:一个组消费一个主题的一个分区是一行
  主键: (groupId, topic, partitionid)
*/

object ExactlyOnceTransaction {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("exactlyone")
    val ssc = new StreamingContext(conf, Seconds(5))
    val groupId="exactlyone"
    val topic = "exactly_one"

    /** 用于把业务计算的结果和对应的偏移量写入Mysql数据库中
     * @param result 业务计算的结果
     * @param ranges offset
     */
    def writeResultAndOffsets(result: Array[(String, Int)], ranges: Array[OffsetRange]): Unit = {
      //写入result
      val sql1 =
        """
          |insert into wordcount values (?,?)
          |on duplicate key update count=count+values(count)
          |""".stripMargin
      val sql2 =
        """
          |replace into offsets values (?,?,?,?)
          |""".stripMargin
      var connection: Connection=null
      var ps1:PreparedStatement =null
      var ps2:PreparedStatement =null
      try {
        connection= JDBCUtil.getConnection()

        /**4.开启事物,提交数据
         */
        //(1)开启事物,关闭自动提交
        /*设置隔离级别
        事物特性：ACID,原子性、一致性、隔离性、持久性
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
        关闭只读
        connection.setReadOnly(false)
        */
        connection.setAutoCommit(false)
        ps1 = connection.prepareStatement(sql1)
        ps2 = connection.prepareStatement(sql2)
        //每遍历一次就会生成一条sql语句,可以一条一条执行,也可以攒起来一起执行
        for((word,count) <- result){
          ps1.setString(1,word)
          ps1.setLong(2,count)
          ps1.addBatch()//攒起来一起执行
        }
        for(offsetrange <- ranges){
          ps2.setString(1,groupId)
          ps2.setString(2,topic)
          ps2.setInt(3,offsetrange.partition)
          ps2.setLong(4,offsetrange.untilOffset)
          ps2.addBatch()
        }
        val res1: Array[Int] = ps1.executeBatch() //批量执行
        val res2 = ps2.executeBatch()
        //(2)提交事物
        connection.commit();
        println("单词数据写入了"+res1.size+"条")
        println("偏移量数据写入了"+res2.size+"条")
      } catch {
        case e:Exception => {
          //(3)事物回滚
          connection.rollback()
          print("写入失败！")
        }
        case _ =>{
          if(ps1!=null) ps1.close()
          if(ps2!=null) ps2.close()
          if(connection!=null) connection.close()
        }
      }
    }
    /**1.查询之前写入数据库的偏移量Offsets */
    //  (1)定义一个函数获取offset：使用jdbc连接mysql,从对应的表中查询数据
    /**
     * 定义一个查询偏移量的方法
     * @param groupId 消费者组id
     * @param topic 对应的主题
     * @return 返回查询的分区和偏移量
     */
    def selectOffsets(groupId:String,topic:String): Map[TopicPartition,Long] ={

      //val offsets = TrieMap[TopicPartition, Long]() 线程安全的可变集合
      //val offsets = mutable.HashMap[TopicPartition,Long]() //采用可变集合
      //Map是不可变集合,添加元素会返回新的集合,因此用var修饰
      var offsets = Map[TopicPartition, Long]()
      val sql =
        """
          |select
          |partitionId,
          |offset
          |from offsets
          |where groupId=? and topic =?
          |""".stripMargin
      var connection: Connection = null
      var ps: PreparedStatement =null
      try {
        connection = JDBCUtil.getConnection()
        ps = connection.prepareStatement(sql)
        ps.setString(1,groupId)
        ps.setString(2,topic)
        val resultset: ResultSet = ps.executeQuery()
        while (resultset.next()){
          /*可变集合添加元素
          offsets.put(new TopicPartition(topic,resultset.getInt("partitionId")),resultset.getLong("offset")*/
          offsets = offsets+(new TopicPartition(topic,resultset.getInt("partitionId"))-> resultset.getLong("offset"))
        }
      } catch {
        case e:Exception => print("获取偏移量失败！")
        case _ =>{
          if(ps!=null) ps.close()
          if(connection!=null) connection.close()
        }
      }
      /*可变转不可变
      offsets.toMap*/
      offsets
    }
    //(2)获取offset
    val offsets: Map[TopicPartition, Long] = selectOffsets(groupId,topic)

    /**2.从offsets位置获取一个流 */
    //(1)配置连接参数
    val kafkapara = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "exactlyone",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )
    //(2)从读取的offset中获取流
    val kafkads = KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("exactly_one"), kafkapara,offsets)/**将获取的offsets集合放入参数列表,offsets为空也能正确运行*/
      //当消费者启动时，如果没有在数据库中找到对应的offset信息,可以根据业务需要选择其中的一种类型作为默认的offset："auto.offset.reset"->"earliest"|"latest"|"none"
    )
    /**3.进行业务处理*/
    kafkads.foreachRDD(
      rdd =>{
        //(1)获取偏移量
        val ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //(2)这里编写一个wordcount
        val ds1 = rdd.map(_.value()).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
        val result: Array[(String, Int)] = ds1.collect()
        //(3)自定义方法,提交数据和偏移量
        writeResultAndOffsets(result,ranges)
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }
}
