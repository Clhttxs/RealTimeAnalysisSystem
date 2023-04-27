package RealTimeUtils

import javafx.scene.input.DataFormat

import java.time.{Instant, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

object DateParseUtil {
  val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
  val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  def parseMillTsToDateTime(milliTs:Long):String ={
    val dateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(milliTs), ZoneId.of("Asia/Shanghai"))
    dateTime.format(dateTimeFormatter)
  }

  def parseMillTsToDate(milliTs:Long):String={
    val dateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(milliTs), ZoneId.of("Asia/Shanghai"))
    dateTime.format(dateFormatter)
  }

  /*def main(args: Array[String]): Unit = {

    println(parseMillTsToDate(1655953658000L),parseMillTsToDateTime(1655953658000L))
  }*/
}
