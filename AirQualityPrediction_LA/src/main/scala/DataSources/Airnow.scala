package DataSources

import org.apache.spark.sql.{DataFrame, SparkSession, functions}


object Airnow {

  // Get AQI at time = timeQuery
  def dbReadAirnow (timeQuery: String,
                    timeResolution: String,
                    sparkSession: SparkSession): DataFrame = {

    val schema = "los_angeles"
    val tableName = s"los_angeles_pm25_$timeResolution"
    val cols = List("sensor_id", "timestamp", "aqi", "mean_aqi") // Can be changed into median
    val condition = s"where timestamp = $timeQuery"
    val airnowDF = DBConnection.dbReadData(schema, tableName, cols, condition, sparkSession)
    airnowDF
  }

  // Get AQI at all timestamp
  def dbReadAirnow (timeResolution: String,
                    sparkSession: SparkSession): DataFrame = {
    //timeResolution = hourly or daily or monthly
    val schema = "los_angeles"
    val tableName = s"los_angeles_pm25_$timeResolution"
    val cols = List("sensor_id", "timestamp", "aqi", "mean_aqi") // Can be changed into median
    var airnowDF = DBConnection.dbReadData(schema, tableName, cols, "", sparkSession)
    airnowDF = airnowDF.withColumn("unix_time", functions.unix_timestamp(airnowDF.col("timestamp")))// * nanoSeconds)

    filterNullValue(airnowDF)
  }

  def dbReadAirnowFishNet (sparkSession: SparkSession): DataFrame = {

    val tableName = "airnow_reporting_area"
    val cols = List("reporting_area", "date_observed", "aqi") // Can be changed into median
    val conditions = "where parameter_name='PM2.5' and reporting_area != 'E San Gabriel V-2' AND reporting_area != 'E San Gabriel V-1' AND reporting_area != 'Antelope Vly'"
    var airnowDF = DBConnection.dbReadDataFishNet(tableName, cols,conditions , sparkSession)
    val newNames = Seq("sensor_id","timestamp","aqi")
    airnowDF = airnowDF.toDF(newNames: _*)
    airnowDF = airnowDF.withColumn("unix_time", functions.unix_timestamp(airnowDF.col("timestamp")))// * nanoSeconds)
    airnowDF.filter(airnowDF.col("aqi").isNotNull)
  }

  // Get AQI at all timestamp
  def dbReadAirQuality (tableName: String,
                        cols: List[String],
                        sparkSession: SparkSession): DataFrame = {

//    val conditions = "where parameter_name = 'PM2.5' and reporting_area != 'E San Gabriel V-1' and " +
//      "date_observed >= '2017-01-01 00:00:00' and date_observed <= '2018-03-31 23:00:00'"

    val conditions = "where date_observed <= '2018-03-31 23:00:00' and pm25 is not NULL"

    var aqDF = DBConnection.dbReadData("public", tableName, cols, conditions, sparkSession)
    aqDF = aqDF.withColumn("unix_time", functions.unix_timestamp(aqDF.col("date_observed")))// * nanoSeconds)
//    filterNullValue(airnowDF)
    aqDF

  }



  def filterNullValue(airnowDF: DataFrame): DataFrame = {

    var df = airnowDF.filter(airnowDF.col("mean_aqi").isNotNull || airnowDF.col("aqi").isNotNull)
    df = df.withColumn("aqi_new", functions.when(df.col("aqi") > 0.0, df.col("aqi")).otherwise(df.col("mean_aqi")))
      .drop("aqi", "mean_aqi")
      .withColumnRenamed("aqi_new", "aqi")
    df
  }

  def getLatesetTimestamp(sparkSession:SparkSession): String ={
    val airnowDF = DBConnection.getMaxTimestamp(sparkSession)
    airnowDF
  }
}
