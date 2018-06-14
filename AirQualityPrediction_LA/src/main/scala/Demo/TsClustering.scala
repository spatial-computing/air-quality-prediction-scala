package Demo

import java.io.{File, PrintWriter}

import DataSources.Airnow
import Modeling.Clustering
import Utils.Consts
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object TsClustering {

  def clusterTest(sparkSession: SparkSession): Unit = {

//    val table = "airnow_reporting_area"
//    val cols = List("reporting_area", "date_observed", "aqi")

    val table = "beijing_air_quality"
    val cols = List("station_id", "date_observed", "pm25")
    val airQuality = Airnow.dbReadAirQuality(table, cols, sparkSession).cache()
    val stations = airQuality.rdd.map(x => x.getAs[String](cols(0))).distinct().collect()

    Clustering.kmeansClustering(airQuality, Consts.iter, cols(0), "unix_time", cols(2), sparkSession)

//    for (station <- stations) {
//      val training = airQuality.filter(airQuality.col(cols(0)) =!= station)
//      Clustering.kmeansClustering(training, Consts.iter, cols(0), "unix_time", cols(2), sparkSession)
//    }

  }


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkSession = SparkSession
      .builder()
      .appName("spiro")
      .config("spark.master", "local[*]")
      .getOrCreate()

    val stageMetrics = ch.cern.sparkmeasure.StageMetrics(sparkSession)
    stageMetrics.runAndMeasure(clusterTest(sparkSession))
    scala.io.StdIn.readLine()
  }
}
