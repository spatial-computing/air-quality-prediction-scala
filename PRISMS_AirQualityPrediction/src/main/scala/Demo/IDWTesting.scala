package Demo

import MLlib.Evaluation
import Modeling.TimeSeriesPreprocessing
import Utils.{Consts, DBConnectionPostgres, InverseDistanceWeight}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession, functions}
import org.apache.spark.sql.types._

import scala.collection.Map

object IDWTesting {

  def prediction(config: Map[String, Any], sparkSession: SparkSession): Unit = {

    val airQualityTableName = config("air_quality_table_name").asInstanceOf[String]
    val airQualityColumnSet = config("air_quality_column_set").asInstanceOf[List[String]]
    val conditions = config("air_quality_request_condition").asInstanceOf[String]

    val distanceTableName = config("distance_table_name").asInstanceOf[String]
    val distanceColumnSet = config("distance_column_set").asInstanceOf[List[String]]

    val airQualityData = DBConnectionPostgres.dbReadData(airQualityTableName, airQualityColumnSet, conditions, sparkSession)
    val airQualityCleaned = TimeSeriesPreprocessing.dataCleaning(airQualityData, airQualityColumnSet, config).cache()

    val distance = DBConnectionPostgres.dbReadData(distanceTableName, distanceColumnSet, "", sparkSession)

    val stations = airQualityCleaned.rdd.map(x => x.getAs[String](airQualityColumnSet.head)).distinct().collect().toList

    val schema = new StructType()
      .add(StructField("sensor_id", StringType, true))
      .add(StructField("timestamp", TimestampType, true))
      .add(StructField("aqi", DoubleType, true))
      .add(StructField("prediction", DoubleType, true))
      .add(StructField("error", DoubleType, true))
      .add(StructField("sqrError", DoubleType, true))

    var res = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema)

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
