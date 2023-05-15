package Beans

import java.text.SimpleDateFormat
import java.util

case class SaleDetail(
          var order_detail_id:String =null,
          var order_id: String=null,
          var order_status:String=null,
          var create_time:String=null,
          var user_id: String=null,
          var sku_id: String=null,
          var user_gender: String=null,
          var user_age: Int=0,
          var user_level: String=null,
          var sku_price: Double=0D,
          var sku_name: String=null,
          var sku_num: Int=0,
          var dt:String=null,
          var province_id:String = null,
          var province_name:String = null,
          var region_id:String = null,
          var iso_code: String = null,
          var iso_3166_2: String = null
                     ) {
  //辅助构造器
  def this(orderInfo:OrderInfo,orderDetail: OrderDetail) {
    //这个this必须有
    this
    mergeOrderInfo(orderInfo)
    mergeOrderDetail(orderDetail)
  }

  //将ProvinceInfo中的信息合并到当前B对象的属性中
  def mergeProvinceInfo(provinceInfo:ProvinceInfo): Unit ={
    if(provinceInfo != null){
      this.province_id=provinceInfo.id
      this.province_name=provinceInfo.name
      this.region_id=provinceInfo.region_id
      this.iso_code=provinceInfo.iso_code
      this.iso_3166_2=provinceInfo.iso_3166_2
    }
  }

  def mergeOrderInfo(orderInfo:OrderInfo): Unit ={
    if(orderInfo!=null){
        this.order_id=orderInfo.id
        this.order_status=orderInfo.order_status
        this.create_time=orderInfo.create_time
        this.dt=orderInfo.create_date
        this.user_id=orderInfo.user_id
        this.province_id=orderInfo.province_id
    }
  }

  def mergeOrderDetail(orderDetail: OrderDetail): Unit ={
    if(orderDetail!=null){
      this.order_detail_id=orderDetail.id
      this.sku_id=orderDetail.sku_id
      this.sku_name=orderDetail.sku_name
      this.sku_price=orderDetail.order_price.toDouble
      this.sku_num=orderDetail.sku_num.toInt
    }
  }

  def mergeUserInfo(userInfo: UserInfo): Unit ={
    if(userInfo!=null){
      this.user_id=userInfo.id

      val formattor = new SimpleDateFormat("yyyy-MM-dd")
      val date: util.Date = formattor.parse(userInfo.birthday)
      val curTs: Long = System.currentTimeMillis()
      val  betweenMs= curTs-date.getTime
      val age=betweenMs/1000L/60L/60L/24L/365L

      this.user_age=age.toInt
      this.user_gender=userInfo.gender
      this.user_level=userInfo.user_level
    }
  }
}