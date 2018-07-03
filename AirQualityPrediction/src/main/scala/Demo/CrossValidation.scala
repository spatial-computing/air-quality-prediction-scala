package Demo

import java.sql.Timestamp
import java.text.SimpleDateFormat

import Computation.Evaluation
import Modeling._
import Utils.{Consts, DBConnectionPostgres, OutputUtils}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}

import scala.collection.Map

object CrossValidation {

  def prediction(config: Map[String, Any], sparkSession: SparkSession): Unit = {

    val parameterName = config("parameter_name").asInstanceOf[String]
    val k_set = if (parameterName == "PM2.5") Consts.k_pm25_hourly_map
      else if (parameterName == "PM10") Consts.k_pm10_hourly_map
      else if (parameterName == "O3") Consts.k_o3_hourly_map

    val airQualityTableName = config("air_quality_table_name").asInstanceOf[String]
    val airQualityColumnSet = config("air_quality_column_set").asInstanceOf[List[String]]
    val airQualityConditions = config("air_quality_request_condition").asInstanceOf[String]

    var airQualityData = DBConnectionPostgres.dbReadData(airQualityTableName, airQualityColumnSet, airQualityConditions, sparkSession).distinct().cache()
    val airQualityCleaned = TimeSeriesPreprocessing.dataCleaning(airQualityData, airQualityColumnSet, config).cache()
    val airQualityTimeSeries = TimeSeriesPreprocessing.timeSeriesConstruction(airQualityCleaned, airQualityColumnSet, config, sparkSession).cache()

    /*
        Get distinct stations
     */
    val stations = airQualityCleaned.rdd.map(x => x.getAs[String](airQualityColumnSet.head)).distinct().collect().toList

    val geoFeatureColumnSet = config("geo_feature_column_set").asInstanceOf[List[String]]
    val geoFeatures = GeoFeatureConstruction.getGeoFeature(Consts.airnow_reporting_area_geofeature_tablename, config, sparkSession).cache()
    val featureName = GeoFeatureConstruction.getGeoFeatureName(geoFeatures, config)
//    val geoAbstraction = GeoFeatureConstruction.getGeoFeatureVector(stations, geoFeatures, featureName, config, sparkSession).cache()

    var startTime = new Timestamp(1000L)
    var endTime = new Timestamp(1000L)
    if (config("from_time_to_time") == true) {
      val fm = new SimpleDateFormat("yyyy-MM-dd HH:00:00.0")
      startTime = new Timestamp(fm.parse(config("start_time").asInstanceOf[String]).getTime)
      endTime = new Timestamp(fm.parse(config("end_time").asInstanceOf[String]).getTime)
    }

    val airQualityId = airQualityColumnSet.head
    val geoFeatureId = geoFeatureColumnSet.head
    val timeColumn = airQualityColumnSet(1)

    val schema = new StructType()
      .add(StructField(geoFeatureId, StringType))
      .add(StructField(Prediction.predictionTimestampColumnName, TimestampType))
      .add(StructField(Prediction.predictionColumnName, DoubleType))

//    var rmseTotal = 0.0
//    var maeTotal = 0.0
//    var nTotal = 0

    for (target <- stations) {

      val trainingStations = stations.filter(x => x != target)
      val testingStations = stations.filter(x => x == target)

      val trainingAirQuality = airQualityCleaned.filter(airQualityCleaned.col(airQualityId) =!= target).cache()

      /*
          testing data should not be cleaned
       */
      val testingAirQuality = airQualityData.filter(airQualityData.col(airQualityId) === target)

      val trainingTimeSeries = airQualityTimeSeries.filter(airQualityTimeSeries.col(airQualityId) =!= target)

      val trainingGeoFeatures = geoFeatures.filter(geoFeatures.col(geoFeatureId) =!= target)
      val testingGeoFeatures = geoFeatures.filter(geoFeatures.col(geoFeatureId) === target)

      val trainingAbstraction = GeoFeatureConstruction.getGeoFeatureVector(trainingStations, trainingGeoFeatures, featureName, config, sparkSession)
      val k = k_set.asInstanceOf[Map[String, Int]](target)
      val tsCluster = FeatureExtraction.clustering(trainingTimeSeries, k, config)

      val featrueImportance = FeatureExtraction.getFeatureImportance(trainingAbstraction, tsCluster, config)
      val sortedImportantFeatures = FeatureExtraction.getSortedFeatures(featureName, featrueImportance)

      //      val  importantFeatures = FeatureExtraction.getImportantFeatures(featureName, featrueImportance)

      /*
          Get only top 1% important features
       */
      val importantFeatures = sortedImportantFeatures.slice(0, (0.01 * sortedImportantFeatures.length).toInt).map(x => x._2)

      val trainingContext = GeoFeatureConstruction.getGeoFeatureVector(trainingStations, trainingGeoFeatures, importantFeatures, config, sparkSession).cache()
      val testingContext = GeoFeatureConstruction.getGeoFeatureVector(testingStations, testingGeoFeatures, importantFeatures, config, sparkSession).cache()

      /*
          Only test on the time in testing data set
       */
      val times = testingAirQuality
        .filter(testingAirQuality.col(timeColumn) >= startTime and testingAirQuality.col(timeColumn) <= endTime)
        .select(testingAirQuality.col(timeColumn)).distinct()
        .rdd.map(x => x.getAs[Timestamp](timeColumn)).collect()

      var tmpResult = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema)

      for (eachTime <- times) {

        val df = trainingAirQuality.filter(trainingAirQuality.col(timeColumn) === eachTime)

        /*
            When the number of training stations is enough
         */
        if (df.count() >= (stations.size * 0.75).toInt) {

          val t = System.currentTimeMillis()
          val prediction = Prediction.predictionRandomForest(df, airQualityId, trainingContext, geoFeatureId, testingContext, eachTime, config)
            .select(geoFeatureId, Prediction.predictionTimestampColumnName, Prediction.predictionColumnName)

          OutputUtils.outputPredictionResult(prediction, startTime, config)

          println(target + " " + eachTime + " finished in" + (System.currentTimeMillis() - t))
          //          tmpResult = tmpResult.union(prediction)

        }
      }

//      val result = tmpResult.join(testingAirQuality, tmpResult.col("timestamp") === testingAirQuality.col(timeColumn))
//        .select(airQualityId, "timestamp", predictionColumn, valueColumn)
//
//      val (rmseVal, m) = Evaluation.rmse(result, valueColumn, predictionColumn)
//      val (maeVal, n) = Evaluation.mae(result, valueColumn, predictionColumn)
//
//      Prediction.outputPredictionResult(result, startTime, config)
//      println(target, rmseVal, maeVal, m)

//      rmseTotal += rmseVal
//      maeTotal += maeVal
//      nTotal += m

    }
  }
}
