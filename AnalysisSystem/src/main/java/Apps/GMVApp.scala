package Apps

import Beans.OrderInfo
import Constant.{StopGracefullyConstant, TopicConstant}
import RealTimeUtils.{DStreamUtil, DateParseUtil, JDBCUtil}
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.{Connection, PreparedStatement, ResultSet}
import scala.util.parsing.json.JSONObject

/**
 * (累加)聚合运算，无法使用at least once+幂等输出保证精确一次
 * 想要保证精确一次：at least once + 事物
 */
object GMVApp extends BaseApp {
  override var topic: String = TopicConstant.ORDER_INFO
  override var groupId: String = "orderInfo"
  override var appName: String = "GMVApp"
  override var batchDuration: Int = 10
  override var stopName: String = StopGracefullyConstant.STOPGVM

  // 提供方法从数据库中查询当前组消费主题的历史offsets
  def readHistoryOffsets(groupId: String, topic: String): Map[TopicPartition, Long] = {
    var offSets = Map[TopicPartition, Long]()
    val sql=
      """
        |select partitionId,offset
        |from offsets
        |where groupId=? and topic=?
        |""".stripMargin
    var connection: Connection = null
    var ps: PreparedStatement =null
    try {
      connection = JDBCUtil.getConnection()
      ps = connection.prepareStatement(sql)
      ps.setString(1, groupId)
      ps.setString(2, topic)
      val resultSet: ResultSet = ps.executeQuery()
      while (resultSet.next()) {
        offSets = offSets + (new TopicPartition(topic, resultSet.getInt("partitonId")) -> resultSet.getLong("offset"))
      }
    } catch {
      case e:Exception => print("获取偏移量失败！")
      case _ =>{
        if(ps!=null) ps.close()
        if(connection!=null) connection.close()
      }
    }
    offSets
  }

  def parseRecord(rdd: RDD[ConsumerRecord[String, String]]): RDD[OrderInfo] = {
    rdd.mapPartitions(partiton =>{
      partiton.map(record => {
        val orderinfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
        // 时间: create_time": "2022-04-27 03:18:39"
        val localDateTime = DateParseUtil.parseStringToLocalDateTime(orderinfo.create_time)
        orderinfo.create_date = DateParseUtil.localDateTimeToDate(localDateTime)
        orderinfo.create_hour = DateParseUtil.localDateTimeToHour(localDateTime)
        orderinfo
      })
    })
  }

  def writeDataAndOffsetsInCommonTransaction(result: Array[((String, String), Double)], ranges: Array[OffsetRange]) = {
    val dataSql =
      """
        |insert into gmvstats values(?,?,?)
        |on duplicate key update gmv=gmv+values(gmv)
        |""".stripMargin
    val offsetSql =
      """
        |replace into offsets values(?,?,?,?)
        |""".stripMargin

    var connection:Connection=null
    var dataPs:PreparedStatement=null
    var offsetPs:PreparedStatement=null
    try {
      connection = JDBCUtil.getConnection()
      //开启事物
      connection.setAutoCommit(false)
      dataPs = connection.prepareStatement(dataSql)
      offsetPs = connection.prepareStatement(offsetSql)
//      println("连接成功")
      for (((date, hour), gmv) <- result) {
        dataPs.setString(1, date)
        dataPs.setString(2, hour)
        dataPs.setDouble(3, gmv)
        dataPs.addBatch()
      }
//      println("result参数输入成功")
      for (offset <- ranges) {
        offsetPs.setString(1, groupId)
        offsetPs.setString(2, topic)
        offsetPs.setInt(3, offset.partition)
        offsetPs.setLong(4, offset.untilOffset)
        offsetPs.addBatch()
      }
//      println("ranges参数输入成功")
      val dataRes: Array[Int] = dataPs.executeBatch()
//      println("data执行成功")
      val offsetRes = offsetPs.executeBatch()
//      println("偏移量提交成功")
      //提交事物
      connection.commit()
      println("Gmv成功写入" + dataRes.size + "条数据！")
      println("偏移量成功写入" + dataRes.size + "条数据！")
    } catch {
      case e:Exception =>{
        //回滚事务
        connection.rollback()
        e.printStackTrace()
        println("发生错误,写入失败!")
      }
      case _ => {
        if(dataPs!=null) dataPs.close()
        if(offsetPs!=null) offsetPs.close()
        if(connection!=null) connection.close()
    }
    }
  }

  def main(args: Array[String]): Unit = {
    context = new StreamingContext("local[*]",appName,Seconds(batchDuration))
    runApp{
      //查询之前消费的Offsets
      val offsetsMap: Map[TopicPartition, Long] = readHistoryOffsets(groupId, TopicConstant.ORDER_INFO)
      val ds = DStreamUtil.createDStream(context, topic, groupId, "false", "earliest", true, offsetsMap)

      /**
       * 不要直接ds.print()：ConsumeRecord需要序列化，
       * SparkConf sparkConf = new SparkConf().setAppName("JavaFlumeEventCount")
                                              .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
       * sparkConf.registerKryoClasses((Class<?>[]) Arrays.asList(ConsumerRecord.class).toArray())
       */
      //ds.map(_.value()).print()
      ds.foreachRDD(rdd =>{
        if(!rdd.isEmpty()){
          //获取偏移量
          val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          //处理数据
          //封装rdd为样例类
          val orderinfo:RDD[OrderInfo] = parseRecord(rdd)
          //计算结果
          val result: Array[((String, String), Double)] = orderinfo
            .map(orderinfo => ((orderinfo.create_date, orderinfo.create_hour), orderinfo.total_amount))
            .reduceByKey(_ + _).collect()
          //result.foreach(println(_))
          // 将计算的结果和offsets在一个事务中一起写入到Mysql
          writeDataAndOffsetsInCommonTransaction(result,ranges)
        }
      })
    }
  }
}
