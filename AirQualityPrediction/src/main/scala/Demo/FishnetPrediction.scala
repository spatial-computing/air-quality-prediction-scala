package Demo

import java.sql.Timestamp
import java.text.SimpleDateFormat

import Modeling._
import Utils.{Consts, DBConnectionMongoDB, DBConnectionPostgres, OutputUtils}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import scala.collection.Map

object FishnetPrediction {

  def prediction(config: Map[String, Any], sparkSession: SparkSession): Unit = {

    val airQualityTableName = config("air_quality_table_name").asInstanceOf[String]
    val airQualityColumnSet = config("air_quality_column_set").asInstanceOf[List[String]]
    val conditions = config("air_quality_request_condition").asInstanceOf[String]

    val airQualityData = DBConnectionPostgres.dbReadData(airQualityTableName, airQualityColumnSet, conditions, sparkSession).distinct()
    val airQualityCleaned = TimeSeriesPreprocessing.dataCleaning(airQualityData, airQualityColumnSet, config).cache()
    val airQualityTimeSeries = TimeSeriesPreprocessing.timeSeriesConstruction(airQualityCleaned, airQualityColumnSet, config, sparkSession).cache()

    /*
          Get distinct stations
       */
    val stations = airQualityCleaned.rdd.map(x => x.getAs[String](airQualityColumnSet.head)).distinct().collect().toList

    val geoFeatureColumnSet = config("geo_feature_column_set").asInstanceOf[List[String]]
    val geoFeatures = GeoFeatureConstruction.getGeoFeature(Consts.airnow_reporting_area_geofeature_tablename, config, sparkSession).cache()
    val featureName = GeoFeatureConstruction.getGeoFeatureName(geoFeatures, config)
    val geoAbstraction = GeoFeatureConstruction.getGeoFeatureVector(stations, geoFeatures, featureName, config, sparkSession)

    val fishnetTableName = config("fishnet_table_name").asInstanceOf[String]
    val fishnetColumnSet = config("fishnet_column_set").asInstanceOf[List[String]]
    val fishnetId = fishnetColumnSet.head
    val fishnet = DBConnectionPostgres.dbReadData(fishnetTableName, fishnetColumnSet, "", sparkSession).cache()
    val fishnetGid = fishnet.rdd.map(x => x.getAs[String](fishnetId)).distinct().collect().toList
    val fishnetGeoFeatures = GeoFeatureConstruction.getGeoFeature(Consts.la_fishnet_geofeature_tablename, config, sparkSession)

    val k = config("kmeans_k").asInstanceOf[Double].toInt
    val tsCluster = FeatureExtraction.clustering(airQualityTimeSeries, k, config)
    val featureImportance = FeatureExtraction.getFeatureImportance(geoAbstraction, tsCluster, config)
    val importantFeatures = FeatureExtraction.getImportantFeatures(featureName, featureImportance)

    val airQualityId = airQualityColumnSet.head
    val geoFeatureId = geoFeatureColumnSet.head
    val timeColumn = airQualityColumnSet(1)

    val trainingContext = GeoFeatureConstruction.getGeoFeatureVector(stations, geoFeatures, importantFeatures, config, sparkSession)
    var testingContext = GeoFeatureConstruction.getGeoFeatureVector(fishnetGid, fishnetGeoFeatures, importantFeatures, config, sparkSession)
    testingContext = testingContext.join(fishnet, testingContext.col(geoFeatureId) === fishnet.col(fishnetId)).cache()

    /*
        Predict for current time
     */
    if (config("current") == true) {

      val maxTimestamp = DBConnectionPostgres.dbReadData(airQualityTableName, List(s"max(${airQualityColumnSet(1)}) as max_timestamp"), conditions, sparkSession)
        .rdd.map(x => x.getAs[Timestamp]("max_timestamp")).collect()(0)

      val df = airQualityCleaned.filter(airQualityCleaned.col(timeColumn) === maxTimestamp)

      /*
           When the number of training stations is enough
       */
      if (df.count() >= (stations.length * 0.75).toInt) {
        val result = Prediction.predictionRandomForest(df, airQualityId, trainingContext, geoFeatureId, testingContext, maxTimestamp, config)
        OutputUtils.writePredictionResult(result, maxTimestamp, config)
      }
    }

    if (config("from_time_to_time") == true) {

      val fm = new SimpleDateFormat("yyyy-MM-dd HH:00:00.0")
      val startTime = new Timestamp(fm.parse(config("start_time").asInstanceOf[String]).getTime)
      val endTime = new Timestamp(fm.parse(config("end_time").asInstanceOf[String]).getTime)

      var times = airQualityCleaned
        .filter(airQualityCleaned.col(timeColumn) >= startTime and airQualityCleaned.col(timeColumn) <= endTime)
        .select(airQualityCleaned.col(timeColumn)).distinct()
        .rdd.map(x => x.getAs[Timestamp](timeColumn)).collect()

      times = OutputUtils.checkExistingTimes(times, config)
      println(times.length)

      for (eachTime <- times) {

        val df = airQualityCleaned.filter(airQualityCleaned.col(timeColumn) === eachTime)

        /*
           When the number of training stations is enough
        */
        if (df.count() >= (stations.length * 0.75).toInt) {

          val prediction = Prediction.predictionRandomForest(df, airQualityId, trainingContext, geoFeatureId, testingContext, eachTime, config)
          OutputUtils.writePredictionResult(prediction, eachTime, config)
          println(eachTime + " finished")

          //          Prediction.outputPredictionResult(result, eachTime, config)
        }
      }
    }
  }


}
