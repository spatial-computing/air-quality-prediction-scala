package ClusteringTest

import Modeling.{FeatureExtraction, TimeSeriesPreprocessing}
import Utils.{Consts, DBConnectionPostgres}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.collection.Map
import scala.io.Source
import scala.util.parsing.json.JSON

object KmeansTesting {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkSession = SparkSession
      .builder()
      .appName("spiro")
      .config("spark.master", "local[*]")
      .getOrCreate()
    /*
        Get configuration from json file
     */
    val configFile = "src/data/model/config_yijun.json"
    val lines = Source.fromFile(configFile).mkString.replace("\n", "")
    val config = JSON.parseFull(lines).get.asInstanceOf[Map[String, Any]]
    if (config.isEmpty) {
      println("Fail to parse json file.")
      return
    }

    val airQualityTableName = config("air_quality_table_name").asInstanceOf[String]
    val airQualityColumnSet = config("air_quality_column_set").asInstanceOf[List[String]]
    val conditions = config("air_quality_request_condition").asInstanceOf[String]

    val airQualityData = DBConnectionPostgres.dbReadData(airQualityTableName, airQualityColumnSet, conditions, sparkSession).distinct().cache()
    val airQualityCleaned = TimeSeriesPreprocessing.dataCleaning(airQualityData, airQualityColumnSet, config).cache()

    val stations = airQualityCleaned.rdd.map(x => x.getAs[String](airQualityColumnSet.head)).distinct().collect().toList
    val airQualityTimeSeries = TimeSeriesPreprocessing.timeSeriesConstruction(airQualityCleaned, airQualityColumnSet, config, sparkSession).cache()

    for (target <- stations) {

      println("Now testing area " + target)
      val trainingStations = stations.filter(x => x != target)

      val trainingTimeSeries = airQualityTimeSeries.filter(airQualityTimeSeries.col(airQualityColumnSet.head) =!= target)

      for (k <- 2 to trainingStations.size) {
        val tsCluster = FeatureExtraction.clustering(trainingTimeSeries, k, config)

      }
    }
  }
}
