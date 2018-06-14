package DataSources

import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object Weather {

  def dbReadWeather (timeQuery: String,
                    timeResolution: String,
                    sparkSession: SparkSession): DataFrame = {

    val schema = "los_angeles"
    val tableName = s"los_angeles_weather_$timeResolution"
    val cols = List("sensor_id", "timestamp", "temp", "humidity", "dewpoint", "windbearing", "windspeed") // Can be changed into median
    val condition = s"where timestamp = $timeQuery"
    val airnowDF = DBConnection.dbReadData(schema, tableName, cols, condition, sparkSession)
    airnowDF
  }

  def dbReadWeather (timeResolution: String,
                    sparkSession: SparkSession): DataFrame = {

    val schema = "los_angeles"
    val tableName = s"los_angeles_weather_$timeResolution"
    val cols = List("sensor_id", "timestamp", "temp", "humidity", "dewpoint", "windbearing", "windspeed") // Can be changed into median
    var weatherDF = DBConnection.dbReadData(schema, tableName, cols, "", sparkSession)
    weatherDF = weatherDF.withColumn("unix_time", functions.unix_timestamp(weatherDF.col("timestamp")))
    weatherDF
  }
}
