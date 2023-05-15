import RealTimeUtils.DateParseUtil

import java.time.LocalDate

object Test {
  def main(args: Array[String]): Unit = {
    val ts = System.currentTimeMillis()
    println(ts)
    println(DateParseUtil.parseMillTsToDateMinute(ts))
  }
}
