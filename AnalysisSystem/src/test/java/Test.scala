import RealTimeUtils.DateParseUtil

object Test {
  def main(args: Array[String]): Unit = {
    println(DateParseUtil.parseMillTsToDateTime(12345613134561L))
  }
}
