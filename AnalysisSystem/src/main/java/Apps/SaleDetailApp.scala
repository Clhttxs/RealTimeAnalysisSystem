package Apps

import Beans.{OrderDetail, OrderInfo, ProvinceInfo, SaleDetail, UserInfo}
import CommonUtils.{PropertiesUtil, RedisUtil}
import Constant.{PrefixConstant, StopGracefullyConstant, TopicConstant}
import RealTimeUtils.{DStreamUtil, DateParseUtil}
import com.alibaba.fastjson.JSON
import com.google.gson.Gson
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.time.LocalDate
import java.util
import java.util.Properties
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/** at least once +幂等输出保证精确一次。
 * 双流join：
 * 原则:谁早到，谁就写入缓存。
 * 1.对于每一个批次的order_info
     (1)和当前批次已经达到的order_detail关联
     (2)到缓存中去之前已经早到的order_detail，找到就关联
     (3)把自己写入缓存，以防后续有晚到的Order_detail
 * 2.对于每一个批次的Order_detail
     (4)和当前批次已经达到的order_info关联。如果Order_info做过此类操作，这里可以省略
     (5)无法关联的order_detail需要读取缓存中早到的order_info，读到就关联
     (6)如果(5)读不到，说明当前的order_detail早到了，将其写入缓存。等待后续批次到达的order_info，再关联。
 */
object SaleDetailApp extends BaseApp {
  override var topic: String = TopicConstant.ORDER_INFO
  override var groupId: String = "SaleDetail"
  override var appName: String = "SaleDetailApp"
  override var batchDuration: Int = 10
  override var stopName: String = StopGracefullyConstant.StopSaleDetail
  val topicDetail=TopicConstant.ORDER_DETAIL

  /**
   * 查询Mysql,获得省份表的信息
   * JDBC:获取连接，准备sql，预编译，查询结果得到resultSet，封装到Map中
   * sparksql:提供查询JDBC的方式，封装好
   */
  def queryProvinceInfo(conf: SparkConf): Map[String, ProvinceInfo] = {
    val map = mutable.HashMap[String, ProvinceInfo]()
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val properties = new Properties()
    properties.setProperty("driver",PropertiesUtil.getProperty("jdbc.driver.name"))
    properties.setProperty("user",PropertiesUtil.getProperty("jdbc.user"))
    properties.setProperty("password",PropertiesUtil.getProperty("jdbc.password"))
    val df: DataFrame = session.read.format("jdbc").jdbc(PropertiesUtil.getProperty("jdbc.url"), "base_province", properties)
    import session.implicits._
    val ds: Dataset[ProvinceInfo] = df.as[ProvinceInfo]
    ds.collect().foreach(province =>{
      map.put(province.id,province)
    })
    map.toMap
  }

  def parseRecord(rdd: RDD[ConsumerRecord[String, String]]): RDD[(String, OrderInfo)] = {
    rdd.mapPartitions(partiton => {
      partiton.map(record => {
        val orderinfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
        // 时间: create_time": "2022-04-27 03:18:39"
        val localDateTime = DateParseUtil.parseStringToLocalDateTime(orderinfo.create_time)
        orderinfo.create_date = DateParseUtil.localDateTimeToDate(localDateTime)
        orderinfo.create_hour = DateParseUtil.localDateTimeToHour(localDateTime)
        (orderinfo.id, orderinfo)
      })
    })
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(appName)
    //采用ES进行存储，需要在SparkConf中配置相关参数
    //配置ES的集群地址和端口号
    conf.set("es.nodes",PropertiesUtil.getProperty("es.nodes"))
    conf.set("es.port",PropertiesUtil.getProperty("es.port"))
    //允许自动创建索引
    conf.set("es.index.auto.create", "true")
    //允许将主机名转化成ip
    conf.set("es.nodes.wan.only", "true")
    context = new StreamingContext(conf,Seconds(batchDuration))
    //获取省份信息
    val provinceMap: Map[String, ProvinceInfo] = queryProvinceInfo(conf)
    //广播省份信息
    val provinceBc: Broadcast[Map[String, ProvinceInfo]] = context.sparkContext.broadcast(provinceMap)
    runApp{
      //获取双流order_info和order_detail
      val dsInfo = DStreamUtil.createDStream(context, topic, groupId)
      val dsDetail: InputDStream[ConsumerRecord[String, String]] = DStreamUtil.createDStream(context, topicDetail, groupId)
      //获取偏移量
      //两个偏移量
      var orderInfoRanges: Array[OffsetRange] = null
      var orderDetailRanges: Array[OffsetRange] = null
      //要保证全部join上，因此得采用full join。三种情况：（info，detail）、（null，detail）、（info，null）
      //order_info和order_detail是1对N的关系
      //对kafkardd进行join操作，需要先转换成kv类型的rdd
      //先转换成样例类然后再转换成kv类型的rdd
      val info: DStream[(String, OrderInfo)] = dsInfo.transform(rdd =>{
        orderInfoRanges=rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        parseRecord(rdd)
      })
      val detail = dsDetail.transform(rdd =>{
        orderDetailRanges=rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.map(record =>{
          val detail = JSON.parseObject(record.value(), classOf[OrderDetail])
          (detail.order_id, detail)
        })
      })
      //info.print(100)
      //detail.print(100)
      val ds: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = info.fullOuterJoin(detail)
      //ds.print(100)
      val dsSale: DStream[SaleDetail] = ds.mapPartitions(partition => {
        val list: ListBuffer[SaleDetail] = ListBuffer()
        val jedis = RedisUtil.getJedis
        val gson = new Gson()
        partition.foreach{
          case((order_id, (orderInfo, orderDetail))) =>{
            //orderInfo.isDefined
            if(orderInfo!=None){
              val valueInfo = orderInfo.get
              if(orderDetail!=None){
                //(1)和当前批次已经达到的order_detail关联
                list.append(new SaleDetail(valueInfo,orderDetail.get))
              }
              //(2)到缓存中去之前已经早到的order_detail，找到就关联
              val details: util.Set[String] = jedis.smembers(PrefixConstant.order_detail_redis_preffix + order_id)
              details.forEach(valueDetail =>{
                list.append(new SaleDetail(valueInfo,JSON.parseObject(valueDetail,classOf[OrderDetail])))
              })
              //(3)无条件把自己写入缓存，以防后续有晚到的Order_detail
              jedis.setex(PrefixConstant.order_info_redis_preffix+order_id,PrefixConstant.max_delay_time,gson.toJson(valueInfo))
            }else {
              //(4)和当前批次已经达到的order_info关联。如果Order_info做过此类操作，这里可以省略
              //(5)无法关联的order_detail需要读取缓存中早到的order_info，读到就关联
              val str: String = jedis.get(PrefixConstant.order_info_redis_preffix + order_id)
              val detail1: OrderDetail = orderDetail.get
              if(str!=null){
                val info1: OrderInfo = JSON.parseObject(str, classOf[OrderInfo])
                list.append(new SaleDetail(info1,detail1))
              }else{
                //(6)如果(5)读不到，说明当前的order_detail早到了，将其写入缓存。等待后续批次到达的order_info，再关联。
                jedis.sadd(PrefixConstant.order_detail_redis_preffix+order_id,gson.toJson(detail1))
                jedis.expire(PrefixConstant.order_detail_redis_preffix+order_id,PrefixConstant.max_delay_time)
              }
            }
          }
        }
        jedis.close()
        list.iterator
      })
      //dsSale.print(100)
      //关联用户和省份
      val dsSale1: DStream[SaleDetail] = dsSale.mapPartitions(partition => {
        val jedis = RedisUtil.getJedis
        /**这段代码是错误的，会返回一个空的Ds流，原因如下：
        1.mapPartitions函数中的partition迭代器已经被遍历完了
        partition.foreach(saleDetail => {
          //获取用户信息
          //获取省份信息
        })
        jedis.close()
        在函数执行完毕返回partition，是无法遍历到任何元素的。这就是产生无数据问题的原因。
        partition
         */
        val saleDetails: Iterator[SaleDetail] = partition.map(saleDetail => {
          //获取用户信息
          val str = jedis.get(PrefixConstant.user_info_redis_preffix + saleDetail.user_id)
          val userInfo: UserInfo = JSON.parseObject(str, classOf[UserInfo])
          //获取省份信息
          val provinceMap: Map[String, ProvinceInfo] = provinceBc.value
          val provinceInfo: ProvinceInfo = provinceMap.get(saleDetail.province_id).get
          saleDetail.mergeUserInfo(userInfo)
          saleDetail.mergeProvinceInfo(provinceInfo)
          saleDetail
        })
        jedis.close()
        saleDetails
      })
      //写入ES
      import org.elasticsearch.spark._
      dsSale1.print(100)
      dsSale1.foreachRDD(rdd =>{
        println("即将写入:"+rdd.count())
        rdd.saveToEs("realtime2022_sale_detail_"+LocalDate.now,Map("es.mapping.id" -> "order_detail_id"))
        //提交偏移量
        dsInfo.asInstanceOf[CanCommitOffsets].commitAsync(orderInfoRanges)
        dsDetail.asInstanceOf[CanCommitOffsets].commitAsync(orderDetailRanges)
      })
    }
  }
}
