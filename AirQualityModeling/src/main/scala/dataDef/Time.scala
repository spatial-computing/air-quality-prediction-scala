package dataDef
import java.text.SimpleDateFormat

import org.apache.spark.sql.catalyst.expressions.UnixTime
import org.joda.time.DateTime



class Time (_time: String) extends java.io.Serializable {

  var intTime = strToTime()
  var dateTime = new DateTime(intTime)
  var year = dateTime.getYear
  var month = dateTime.getMonthOfYear
  var day = dateTime.getDayOfMonth
  var hour = dateTime.getHourOfDay
  var minute = dateTime.getMinuteOfHour  // =0
  var second = dateTime.getSecondOfMinute  // =0

  def strToTime(): Long = {
    val inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    return (inputFormat.parse(_time)).getTime
  }
}
