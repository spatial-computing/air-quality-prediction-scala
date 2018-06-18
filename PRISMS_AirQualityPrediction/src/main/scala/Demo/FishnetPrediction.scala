package Demo

import java.sql.Timestamp

import DataSources._
import Modeling.{FeatureTransforming, Modeling, SparkML}
import Utils.Consts
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import scala.collection.Map

object FishnetPrediction {


  def fishnetPrediction(config: Map[String, Any], sparkSession: SparkSession): Unit = {

    val airQualityCols = config("Air_Quality_Cols").asInstanceOf[List[String]]
    val fishnet = config("Fishnet").asInstanceOf[String]
    val fishnetCols = config("Fishnet_Cols").asInstanceOf[List[String]]
    val unixTimeCol = config("Unix_Time_Col").asInstanceOf[String]
    val labelCol = config("Output_Label_Col").asInstanceOf[String]
    val geoFeatureCol = config("Output_Geo_Feature_Col").asInstanceOf[String]
    val scaledFeatureCol = config("Output_Scaled_Feature_Col").asInstanceOf[String]
    val resultCol = config("Output_Result_Col").asInstanceOf[String]

    val (airQualityCleaned_, airQualityTimeSeries_) = AirQualityData.getAirQualityTimeSeries(config, sparkSession)
    val airQualityCleaned = airQualityCleaned_.cache()
    val airQualityTimeSeries = airQualityTimeSeries_.cache()

//    val stationCol = airQualityTimeSeries.schema.fields.head.name
    val stations = airQualityTimeSeries.rdd.map(x => x.getAs[String](airQualityCols.head)).distinct().collect().toList

    val sensorGeoFeatures = GeoFeatureData.geoFeatureConstruction(Consts.airnow_reporting_area_geofeature_tablename, config, sparkSession)
    val featureName = GeoFeatureUtils.getFeatureNames(sensorGeoFeatures, config)
    val geoAbstraction = GeoFeatureUtils.getGeoAbstraction(stations, sensorGeoFeatures, config, featureName, sparkSession).cache()

    val fishnetGid = DBConnection.dbReadData(fishnet, fishnetCols, "", sparkSession)
                     .rdd.map(x => x.getAs[String](fishnetCols.head)).distinct().collect().toList

    val fishnetGeoFeatures = GeoFeatureData.geoFeatureConstruction(Consts.la_fishnet_geofeature_tablename, config, sparkSession)


    val (sensorContext, fishnetContext) = modeling(airQualityTimeSeries, stations, fishnetGid, geoAbstraction, featureName,
      sensorGeoFeatures, fishnetGeoFeatures, airQualityCols, fishnetCols, labelCol, geoFeatureCol, scaledFeatureCol, resultCol,
      config, sparkSession)

    /*
        Predict for current time
     */
    if (config("Current") == true) {

      val maxTimestamp = AirQualityData.getLatestTimestamp(config, "max_timestamp", sparkSession)
      val dt = airQualityCleaned.join(maxTimestamp, airQualityCleaned.col(airQualityCols(1)) === maxTimestamp.col("max_timestamp"))

      if (dt.count() <= 10)
        return

      val result = Modeling.predictionRF(dt, sensorContext, fishnetContext, scaledFeatureCol, labelCol, resultCol, config)
        .select(fishnetCols.head, airQualityCols(1), resultCol)

      if (config("Write_to_DB") == true)
        DBConnection.dbWriteData(result, "others", "prediction_result")
    }

    if (config("From_Time_to_Time") == true) {


      val times = airQualityCleaned.select(airQualityCleaned.col(airQualityCols(1))).distinct()
        .rdd.map(x => x.getAs[Timestamp](airQualityCols(1))).collect()

      for (eachTime <- times) {

        val dt = airQualityCleaned.filter(airQualityCleaned.col(airQualityCols(1)) === eachTime)
        if (dt.count() >= 10) {

          val result = Modeling.predictionRF(dt, sensorContext, fishnetContext, scaledFeatureCol, labelCol, resultCol, config)
            .select(fishnetCols.head, fishnetCols.head, resultCol)
            .withColumn("timestamp", functions.lit(eachTime))
            .select(fishnetCols.head, "timestamp", resultCol)

          if (config("Write_to_DB") == true)
            DBConnection.dbWriteData(result, "others", "prediction_result")
        }
      }
    }
  }

  def modeling(timeSeries: DataFrame,
               trainingLoc: List[String], testingLoc: List[String],
               geoAbstraction: DataFrame, featureName: RDD[(String, String, Int)],
               trainingGeoFeatures: DataFrame, testingGeoFeatures: DataFrame,
               trainingCols: List[String], testingCols: List[String], labelCol: String,
               geoFeatureCol: String, scaledFeatureCol: String, resultCol: String,
               config: Map[String, Any], sparkSession: SparkSession):

  (DataFrame, DataFrame) = {

    /*
        Clustering on time series data
     */
    val k = config("Kmeans_K").asInstanceOf[Double].toInt
    val ssTimeSeries = FeatureTransforming.standardScaler(timeSeries, labelCol, scaledFeatureCol)
    val tsCluster = SparkML.kMeans(ssTimeSeries, scaledFeatureCol, "cluster", k, 100)

    /*
        Get important features
     */
    val featureImportance = Modeling.getFeatureImportance(geoAbstraction, tsCluster, geoFeatureCol, "cluster", "prediction", config)
    val importantFeaturesName = GeoFeatureUtils.getImportantFeaturesName(featureName, featureImportance)

    /*
        Get geo-context
     */
    val trainingContext = GeoFeatureUtils.getGeoContext(trainingGeoFeatures, trainingCols.head, trainingLoc, importantFeaturesName, config, sparkSession)
    val testingContext = GeoFeatureUtils.getGeoContext(testingGeoFeatures, testingCols.head, testingLoc, importantFeaturesName, config, sparkSession)

    val ssTrainingContext = FeatureTransforming.standardScaler(trainingContext, geoFeatureCol, scaledFeatureCol).cache()
    val ssTestingContext = FeatureTransforming.standardScaler(testingContext, geoFeatureCol, scaledFeatureCol).cache()

    (ssTrainingContext, ssTestingContext)
  }
}
