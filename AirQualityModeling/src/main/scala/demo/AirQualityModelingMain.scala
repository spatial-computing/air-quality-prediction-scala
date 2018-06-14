package demo

import connection._
import dataDef.{GeoFeature, Time}
import dataIO.ClusterResultIO
import databaseIO.{AirnowIO, ClimateIO, GeoIO}
import modeling.{DataPreprocessing, FeatureImportance, GeoContext, Prediction}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object AirQualityModelingMain {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder
      .appName("AqiPrediction")
      .config("spark.master", "local")
      .getOrCreate()

    val sc = spark.sparkContext


    // Initial time and location (lon, lat)
    val timeQuery = "2016-11-03 19:00:00-07"
    val timeResolution = "hourly"
    val lon = -118.4566
    val lat = 34.0505

    val time = new Time(timeQuery)
    // Connect to database
    val connection = ConnectDB.connectdb(spark)

    // Get sensor data (reporting area, time, aqi)
    val airnowRDD = AirnowIO.readAirnowDataFromDB(timeQuery, timeResolution, spark, connection)
    // Get clustering result
    val fileName = "./data/clusterResult.txt"
    val aqiClusterResultRDD = ClusterResultIO.readClusterResult(fileName, spark)

    //    val climateRDD = ClimateIO.readClimateDataFromDB(spark, connection)

    // Get all available reporting areas
    val reportingAreas = airnowRDD.map(_.reportingArea).distinct().collect().sorted
    val geoFeature = DataPreprocessing.geoDataPreparation(connection, spark)
    val featureName = geoFeature.getFeatureName()

    val trainingFeatureVector = geoFeature.geoFeatureVectorConstruction(reportingAreas, featureName)

    val featureImportance =
      FeatureImportance.calFeatureImportance(trainingFeatureVector, aqiClusterResultRDD, spark)

    var targetLocation = new Array[String](1)
    targetLocation(0) = "Target Location"

    val targetLandusagesAreaRDD = GeoIO.readGeoDataFromDB(lon, lat, "landusages", spark, connection)
    val targetWaterareasAreaRDD = GeoIO.readGeoDataFromDB(lon, lat, "waterareas", spark, connection)
    val targetRoadsLengthRDD = GeoIO.readGeoDataFromDB(lon, lat, "roads", spark, connection)
    val targetAerowaysLengthRDD = GeoIO.readGeoDataFromDB(lon, lat, "aeroways", spark, connection)
    val targetBuildingsNumRDD = GeoIO.readGeoDataFromDB(lon, lat, "buildings", spark, connection)
    val targetOceanRDD = GeoIO.readGeoDataFromDB(lon, lat, "ocean", spark, connection)
    val targetGeoRDD = new GeoFeature(targetLandusagesAreaRDD, targetWaterareasAreaRDD, targetRoadsLengthRDD,
      targetAerowaysLengthRDD, targetBuildingsNumRDD, targetOceanRDD)
    val targetGeoFeatureVector = targetGeoRDD.geoFeatureVectorConstruction(targetLocation, featureName)


    // Compute geo-context for target location
    // geoContext : ArrayBuffer[(String, Array[Double])] (reporting_area, geo_context)
    val trainingGeoContextRDD = GeoContext.getGeoContext(trainingFeatureVector, featureImportance, spark)
    val targetGeoContextRDD = GeoContext.getGeoContext(targetGeoFeatureVector, featureImportance, spark)


    val predictionResult =
      Prediction.targetPrediction(airnowRDD, time.intTime, trainingGeoContextRDD, targetGeoContextRDD, spark)
        .collect()(0)._2

    println("Location lon = " + lon + "Location lat = " + lat)
    println("AQI = " + predictionResult)

  }
}
