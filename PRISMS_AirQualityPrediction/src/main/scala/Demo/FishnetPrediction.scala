package Demo

import java.sql.Timestamp

import Modeling._
import Utils.{Consts, DBConnectionPostgres}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import scala.collection.Map

object FishnetPrediction {


  def prediction(config: Map[String, Any], sparkSession: SparkSession): Unit = {

    val airQualityTableName = config("air_quality_table_name").asInstanceOf[String]
    val airQualityColumnSet = config("air_quality_column_set").asInstanceOf[List[String]]
    val conditions = config("air_quality_request_condition").asInstanceOf[String]
    val fishnetTableName = config("fishnet_table_name").asInstanceOf[String]
    val fishnetColumnSet = config("fishnet_column_set").asInstanceOf[List[String]]

    val predictionColumn = config("prediction_column").asInstanceOf[String]

    val airQualityData = DBConnectionPostgres.dbReadData(airQualityTableName, airQualityColumnSet, conditions, sparkSession)
    val airQualityCleaned = TimeSeriesPreprocessing.dataCleaning(airQualityData, airQualityColumnSet, config).cache()
    val airQualityTimeSeries = TimeSeriesPreprocessing.timeSeriesConstruction(airQualityCleaned, airQualityColumnSet, config, sparkSession).cache()

    val stations = airQualityTimeSeries.rdd.map(x => x.getAs[String](airQualityColumnSet.head)).distinct().collect().toList

    val sensorGeoFeatures = GeoFeatureConstruction.getGeoFeature(Consts.airnow_reporting_area_geofeature_tablename, config, sparkSession)
    val featureName = GeoFeatureConstruction.getFeatureNames(sensorGeoFeatures, config)
    val geoAbstraction = GeoFeatureConstruction.getGeoAbstraction(stations, sensorGeoFeatures, featureName, config, sparkSession).cache()

    val fishnetGid = DBConnectionPostgres.dbReadData(fishnetTableName, fishnetColumnSet, "", sparkSession)
                     .rdd.map(x => x.getAs[String](fishnetColumnSet.head)).distinct().collect().toList

    val fishnetGeoFeatures = GeoFeatureConstruction.getGeoFeature(Consts.la_fishnet_geofeature_tablename, config, sparkSession)

    val k = config("kmeans_k").asInstanceOf[Double].toInt
    val tsCluster = FeatureExtraction.clustering(airQualityTimeSeries, k, config)
    val featureImportance = FeatureExtraction.getFeatureImportance(geoAbstraction, tsCluster, config)
    val importantFeatures = FeatureExtraction.getImportantFeature(featureName, featureImportance)

    val trainingContext = GeoFeatureConstruction.getGeoContext(stations, sensorGeoFeatures, importantFeatures, config, sparkSession)
    val testingContext = GeoFeatureConstruction.getGeoContext(fishnetGid, fishnetGeoFeatures, importantFeatures, config, sparkSession)

    val testingContextId = testingContext.schema.fields.head.name

    /*
        Predict for current time
     */
    if (config("current") == true) {

      val maxTimestamp = DBConnectionPostgres.dbReadData(airQualityTableName, List(s"max(${airQualityColumnSet(1)}) as max_timestamp"), conditions, sparkSession)
      val dt = airQualityCleaned.join(maxTimestamp, airQualityCleaned.col(airQualityColumnSet(1)) === maxTimestamp.col("max_timestamp"))

      if (dt.count() <= 10)
        return

      val result = Prediction.predictionRandomForest(dt, trainingContext, testingContext, config)
        .select(fishnetColumnSet.head, airQualityColumnSet(1), predictionColumn)

      if (config("write_to_DB") == true)
        DBConnectionPostgres.dbWriteData(result, "others", "prediction_result")
    }

    if (config("from_time_to_time") == true) {

      val times = airQualityCleaned.select(airQualityCleaned.col(airQualityColumnSet(1))).distinct()
        .rdd.map(x => x.getAs[Timestamp](airQualityColumnSet(1))).collect()

      for (eachTime <- times) {

        val dt = airQualityCleaned.filter(airQualityCleaned.col(airQualityColumnSet(1)) === eachTime)
        if (dt.count() >= 10) {

          val result = Prediction.predictionRandomForest(dt, trainingContext, testingContext, config)
            .withColumn("timestamp", functions.lit(eachTime))
            .select(testingContextId, "timestamp", predictionColumn)

          if (config("write_to_DB") == true)
            DBConnectionPostgres.dbWriteData(result, "others", "prediction_result")
        }
      }
    }
  }
}
