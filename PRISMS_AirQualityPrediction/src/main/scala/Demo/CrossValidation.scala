package Demo

import java.sql.Timestamp

import Modeling._
import Utils.{Consts, DBConnection}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}

import scala.collection.Map

object CrossValidation {

  def prediction(config: Map[String, Any], sparkSession: SparkSession): Unit = {

    val airQualityTableName = config("air_quality_table_name").asInstanceOf[String]
    val airQualityColumnSet = config("air_quality_column_set").asInstanceOf[List[String]]
    val conditions = config("air_quality_request_condition").asInstanceOf[String]
    val geoFeatureColumnSet = config("geo_feature_column_set").asInstanceOf[List[String]]

    val predictionColumn = config("prediction_column").asInstanceOf[String]

    val airQualityData = DBConnection.dbReadData(airQualityTableName, airQualityColumnSet, conditions, sparkSession)
    val airQualityCleaned = TimeSeriesPreprocessing.dataCleaning(airQualityData, airQualityColumnSet, config).cache()
    val airQualityTimeSeries = TimeSeriesPreprocessing.timeSeriesConstruction(airQualityCleaned, airQualityColumnSet, config, sparkSession).cache()

    val stations = airQualityTimeSeries.rdd.map(x => x.getAs[String](airQualityColumnSet.head)).distinct().collect().toList

    val sensorGeoFeatures = GeoFeatureConstruction.getGeoFeature(Consts.airnow_reporting_area_geofeature_tablename, config, sparkSession)
    val featureName = GeoFeatureConstruction.getFeatureNames(sensorGeoFeatures, config)

    val schema = new StructType()
      .add(StructField(airQualityColumnSet.head, StringType, true))
      .add(StructField("timestamp", TimestampType, true))
      .add(StructField(predictionColumn, DoubleType, true))

    for (target <- stations) {

      val trainingStations = stations.filter(x => x != target)
      val testingStations = stations.filter(x => x == target)

      val trainingAirQuality = airQualityCleaned.filter(airQualityCleaned.col(airQualityColumnSet.head) =!= target)
      /*
          testing data should not be cleaned
       */
      val testingAirQuality = airQualityData.filter(airQualityCleaned.col(airQualityColumnSet.head) === target)

      val trainingTimeSeries = airQualityTimeSeries.filter(airQualityTimeSeries.col(airQualityColumnSet.head) =!= target)

      val trainingGeoFeatures = sensorGeoFeatures.filter(sensorGeoFeatures.col(geoFeatureColumnSet.head) =!= target)
      val testingGeoFeatures = sensorGeoFeatures.filter(sensorGeoFeatures.col(geoFeatureColumnSet.head) === target)

      val trainingAbstraction = GeoFeatureConstruction.getGeoAbstraction(trainingStations, trainingGeoFeatures, featureName, config, sparkSession).cache()

      val k = Consts.kHourlyMap(target)
      val tsCluster = FeatureExtraction.clustering(trainingTimeSeries, k, config)
      val featrueImportance = FeatureExtraction.getFeatureImportance(trainingAbstraction, tsCluster, config)
      val importantFeatures = FeatureExtraction.getImportantFeature(featureName, featrueImportance)

      val trainingContext = GeoFeatureConstruction.getGeoContext(trainingStations, trainingGeoFeatures, importantFeatures, config, sparkSession)
      val testingContext = GeoFeatureConstruction.getGeoContext(testingStations, testingGeoFeatures, importantFeatures, config, sparkSession)

      /*
          Only test on the time in testing data set
       */
      val times = testingAirQuality.select(testingAirQuality.col(airQualityColumnSet(1))).distinct()
        .rdd.map(x => x.getAs[Timestamp](airQualityColumnSet(1))).collect()

      var result = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema)

      for (eachTime <- times) {

        val dt = trainingAirQuality.filter(trainingAirQuality.col(airQualityColumnSet(1)) === eachTime)
        if (dt.count() >= 10) {

          val prediction = Prediction.predictionRandomForest(dt, trainingContext, testingContext, config)
            .select(airQualityColumnSet.head, predictionColumn)
            .withColumn("timestamp", functions.lit(eachTime))
            .select(airQualityColumnSet.head, "timestamp", predictionColumn)

          result = result.union(prediction)
        }
      }

      val tmp = result.join(testingAirQuality, result.col("timestamp") === testingAirQuality.col(airQualityColumnSet(1)))
      if (config("write_to_db") == true) {
        DBConnection.dbWriteData(tmp, "others", "cross_validation_result")
      }

      if (config("write_to_csv") == true) {
        val tmpDir = s"src/data/result/$target"
        tmp.coalesce(1).write.format("com.databricks.spark.csv").option("header", true).save(tmpDir)
      }
    }
  }
}
