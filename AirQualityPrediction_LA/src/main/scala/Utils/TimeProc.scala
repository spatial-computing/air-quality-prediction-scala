package Utils

import org.apache.spark.sql.functions
import org.joda.time.DateTime

object TimeProc {

  val dayOfWeekUdf = functions.udf((unix_time: Long) => {new DateTime(unix_time * 1000).getDayOfWeek})
  val timeOfDayUdf = functions.udf((unix_time: Long) => {new DateTime(unix_time * 1000).getHourOfDay})

}
