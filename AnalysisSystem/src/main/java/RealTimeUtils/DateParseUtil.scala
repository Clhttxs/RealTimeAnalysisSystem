package RealTimeUtils

import javafx.scene.input.DataFormat

import java.time.{Instant, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

object DateParseUtil {
  val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
  val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val hourFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("HH")
  val MinuteFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm")
  def parseMillTsToDateTime(milliTs:Long):String ={
    val dateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(milliTs), ZoneId.of("Asia/Shanghai"))
    dateTime.format(dateTimeFormatter)
  }

  def parseMillTsToDate(milliTs:Long):String={
    val dateTime: LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(milliTs), ZoneId.of("Asia/Shanghai"))
    dateTime.format(dateFormatter)
  }

  def parseMIllTsToHour(milliTs:Long):String ={
    val hour = LocalDateTime.ofInstant(Instant.ofEpochMilli(milliTs), ZoneId.of("Asia/Shanghai"))
    hour.format(hourFormatter)
  }

  def parseMillTsToDateMinute(milliTs:Long):String ={
    val dateMinute = LocalDateTime.ofInstant(Instant.ofEpochMilli(milliTs), ZoneId.of("Asia/Shanghai"))
    dateMinute.format(MinuteFormatter)
  }

  def parseStringToLocalDateTime(date:String): LocalDateTime ={
    val localDateTime = LocalDateTime.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    localDateTime
  }

  def localDateTimeToDate(localDateTime: LocalDateTime):String ={
    localDateTime.format(dateFormatter)
  }

  def localDateTimeToHour(localDateTime: LocalDateTime):String ={
    localDateTime.format(hourFormatter)
  }

  /*def main(args: Array[String]): Unit = {
    val create_time = "2022-04-27 03:18:39"
    val localDateTime = parseStringToLocalDateTime(create_time)
    println(localDateTimeToDate(localDateTime))
    println(localDateTimeToHour(localDateTime))
  }*/
}
