package Beans

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class CouponAlertInfo(id:String,
                           uids:mutable.Set[String],
                           itemIds:mutable.Set[String],
                           events:ListBuffer[String],
                           ts:Long)