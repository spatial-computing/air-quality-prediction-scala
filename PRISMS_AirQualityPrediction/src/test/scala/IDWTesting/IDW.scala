package IDWTesting

import MLlib.Evaluation
import Modeling.TimeSeriesPreprocessing
import Utils.{DBConnectionPostgres, InverseDistanceWeight}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.Map
import scala.io.Source
import scala.util.parsing.json.JSON

object IDW {

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
    val configFile = "src/data/model/config.json"
    val lines = Source.fromFile(configFile).mkString.replace("\n", "")
    val config = JSON.parseFull(lines).get.asInstanceOf[Map[String, Any]]
    if (config.isEmpty) {
      println("Fail to parse json file.")
      return
    }

    val airQualityTableName = config("air_quality_table_name").asInstanceOf[String]
    val airQualityColumnSet = config("air_quality_column_set").asInstanceOf[List[String]]
    val conditions = config("air_quality_request_condition").asInstanceOf[String]
    val predictionColumn = config("prediction_column").asInstanceOf[String]


    val airQualityData = DBConnectionPostgres.dbReadData(airQualityTableName, airQualityColumnSet, conditions, sparkSession)
    val airQualityCleaned = TimeSeriesPreprocessing.dataCleaning(airQualityData, airQualityColumnSet, config).cache()

    val distance = DBConnectionPostgres.dbReadData("airnow_reporting_area_location_distance", List("id_a", "id_b", "distance"), "", sparkSession)

    val stations = airQualityCleaned.rdd.map(x => x.getAs[String](airQualityColumnSet.head)).distinct().collect().toList

    var rmseTotal = 0.0
    var maeTotal = 0.0
    var nTotal = 0

    for (target <- stations) {

      val trainingStations = stations.filter(x => x != target)
      val testingStations = stations.filter(x => x == target)

      val trainingAirQuality = airQualityCleaned.filter(airQualityCleaned.col(airQualityColumnSet.head) =!= target)
      /*
          testing data should not be cleaned
       */
      val testingAirQuality = airQualityData.filter(airQualityData.col(airQualityColumnSet.head) === target)

      val result = InverseDistanceWeight.idw(testingAirQuality, trainingAirQuality, distance, config, sparkSession)

      val (rmseVal, m) = Evaluation.rmse(result, result.schema.fields(2).name, result.schema.fields(3).name)
      val (maeVal, n) = Evaluation.mae(result, result.schema.fields(2).name, result.schema.fields(3).name)

      println(target, rmseVal, maeVal, m, n)
    }

  }
}
