package Demo

import java.sql.Timestamp

import DataSources._
import Modeling.{FeatureTransforming, PredictionHelper, SparkML}
import Utils.Consts
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}

import scala.collection.Map

object PredictionCV {

  def cvPrediction(config: Map[String, Any], sparkSession: SparkSession): Unit = {

    val airQuality = config("Air_Quality").asInstanceOf[String]
    val airQualityCols = config("Air_Quality_Cols").asInstanceOf[List[String]]
    val conditions = config("Conditions").asInstanceOf[String]
    val geoFeatureCols = config("Geo_Features_Cols").asInstanceOf[List[String]]
    val outputLabelCol = config("Output_Label_Col").asInstanceOf[String]
    val outputGeoFeatureCol = config("Output_Geo_Feature_Col").asInstanceOf[String]
    val outputScaledFeatureCol = config("Output_Scaled_Feature_Col").asInstanceOf[String]
    val outputResultCol = config("Output_Result_Col").asInstanceOf[String]

    val airQualityData_ = DBConnection.dbReadData(airQuality, airQualityCols, conditions, sparkSession)
    val airQualityData = airQualityData_.cache()

    val (airQualityCleaned_, airQualityTimeSeries_) = AirQualityData.getAirQualityTimeSeries(airQualityData, config, sparkSession)
    val airQualityCleaned = airQualityCleaned_.cache()
    val airQualityTimeSeries = airQualityTimeSeries_.cache()

    val stations = airQualityTimeSeries.rdd.map(x => x.getAs[String](airQualityCols.head)).distinct().collect().toList

    val sensorGeoFeatures = GeoFeatureData.geoFeatureConstruction(Consts.airnow_reporting_area_geofeature_tablename, config, sparkSession).cache()
    val featureName = GeoFeatureUtils.getFeatureNames(sensorGeoFeatures, config)

    val schema = new StructType()
      .add(StructField(airQualityCols.head, StringType, true))
      .add(StructField("timestamp", TimestampType, true))
      .add(StructField(outputResultCol, DoubleType, true))

    for (target <- stations) {

      val trainingStations = stations.filter(x => x != target)
      val testingStations = stations.filter(x => x == target)

      val trainingAirQuality = airQualityCleaned.filter(airQualityCleaned.col(airQualityCols.head) =!= target)
      /*
          testing data should not be cleaned
       */
      val testingAirQuality = airQualityData.filter(airQualityCleaned.col(airQualityCols.head) === target)

      val trainingTimeSeries = airQualityTimeSeries.filter(airQualityTimeSeries.col(airQualityCols.head) =!= target)
      val testingTimeSeries = airQualityTimeSeries.filter(airQualityTimeSeries.col(airQualityCols.head) === target)

      val trainingGeoFeatures = sensorGeoFeatures.filter(sensorGeoFeatures.col(geoFeatureCols.head) =!= target)
      val testingGeoFeatures = sensorGeoFeatures.filter(sensorGeoFeatures.col(geoFeatureCols.head) === target)

      val trainingAbstraction = GeoFeatureUtils.getGeoAbstraction(trainingStations, trainingGeoFeatures, featureName, config, sparkSession).cache()

      val (trainingContext, testingContext) = PredictionHelper.modeling(trainingTimeSeries, trainingStations,
        testingStations, trainingAbstraction, featureName, trainingGeoFeatures, testingGeoFeatures, airQualityCols,
        airQualityCols, outputLabelCol, outputGeoFeatureCol, outputScaledFeatureCol, config, sparkSession)

      /*
          Only test on the time in testing data set
       */
      val times = testingAirQuality.select(testingAirQuality.col(airQualityCols(1))).distinct()
        .rdd.map(x => x.getAs[Timestamp](airQualityCols(1))).collect()

      var result = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema)

      for (eachTime <- times) {

        val dt = airQualityCleaned.filter(airQualityCleaned.col(airQualityCols(1)) === eachTime)
        if (dt.count() >= 10) {

          val prediction = PredictionHelper.predictionRF(dt, trainingContext, testingContext, outputScaledFeatureCol,
            outputLabelCol, outputResultCol, config)
            .select(airQualityCols.head, outputResultCol)
            .withColumn("timestamp", functions.lit(eachTime))
            .select(airQualityCols.head, "timestamp", outputResultCol)

          result = result.union(prediction)
        }
      }

      val tmp = result.join(testingAirQuality, result.col(airQualityCols(1)) === testingAirQuality.col(airQualityCols(1)))
      if (config("Write_to_DB") == true) {
        DBConnection.dbWriteData(tmp, "others", "cross_validation_result")
      }

      if (config("Write_to_CSV") == true) {
        val tmpDir = s"src/data/result/$target"
        tmp.coalesce(1).write.format("com.databricks.spark.csv").option("header", true).save(tmpDir)
      }
    }
  }
}
