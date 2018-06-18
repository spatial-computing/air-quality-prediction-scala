package DataSources

import Modeling.TimeSeriesProcessing
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import scala.collection.Map

object AirQualityData {

  def getAirQualityTimeSeries (config: Map[String, Any],
                               sparkSession: SparkSession):
  (DataFrame, DataFrame) = {

    val airQuality = config("Air_Quality").asInstanceOf[String]
    val airQualityCols = config("Air_Quality_Cols").asInstanceOf[List[String]]
    val conditions = config("Conditions").asInstanceOf[String]
    val unixTimeCol = config("Unix_Time_Col").asInstanceOf[String]
    val winOutputValueCol = config("Output_Label_Col").asInstanceOf[String]

    var airQualityData = DBConnection.dbReadData(airQuality, airQualityCols, conditions, sparkSession)

    /*
        Add column "unix_time": represent convert a timestamp to unix time
        Add column mean, median, val: val = ori_val if not null else mean/median
     */
    val airQualityCleaned = TimeSeriesProcessing.timeSeriesPreprocess(airQualityData, airQualityCols, config)

    /*
        Convert column-like air quality data to row-like time series data
        e.g. station, time, val => time1, time2, time3...
     */
    val newCols = List(airQualityCols.head, unixTimeCol, winOutputValueCol)
    val airQualityTimeSeries = TimeSeriesProcessing.timeSeriesConstruction(airQualityCleaned, newCols, sparkSession)

    (airQualityCleaned, airQualityTimeSeries)
  }

  def getLatestTimestamp (config: Map[String, Any], output: String,
                          sparkSession: SparkSession): DataFrame = {

    val airQualityTableName = config("Air_Quality").asInstanceOf[String]
    val airQualityCols = config("Air_Quality_Cols").asInstanceOf[List[String]]
    val conditions = config("Conditions").asInstanceOf[String]
    DBConnection.dbReadData(airQualityTableName, List(s"max(${airQualityCols(1)}) as $output"), conditions, sparkSession)
  }
}
