package demo

import connection.ConnectDB
import dataDef.GeoFeature
import dataIO.ClusterResultIO
import databaseIO._
import modeling._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object FishnetPredictionMain {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder
      .appName("AqiPrediction")
      .config("spark.master", "local")
      .getOrCreate()

    val sc = spark.sparkContext

    // Connect to database
    val connection = ConnectDB.connectdb(spark)

    // Get sensor data (reporting area, time, aqi)
    val timeResolution = "hourly"
    val airnowRDD = AirnowIO.readAirowDataFromDB(timeResolution, spark, connection).cache()
//    val climateRDD = ClimateIO.readClimateDataFromDB(spark, connection)
    // Get clustering result
    val fileName = "./data/clusterResult.txt"
    val aqiClusterResultRDD = ClusterResultIO.readClusterResult(fileName, spark)


    // Get OpenStreetMap data
    val geoFeature = DataPreprocessing.geoDataPreparation(connection, spark)
    val featureName = geoFeature.getFeatureName()

    val fishnetGidRDD = FishnetIO.readFishnetGidFromDB(spark, connection)
    val fishnetLandusagesAreaRDD = FishnetIO.readFishnetDataFromDB("landusages", spark, connection)
    val fishnetWaterareasAreaRDD = FishnetIO.readFishnetDataFromDB("waterareas", spark, connection)
    val fishnetRoadsLengthRDD = FishnetIO.readFishnetDataFromDB("roads", spark, connection)
    val fishnetAerowaysLengthRDD = FishnetIO.readFishnetDataFromDB("aeroways", spark, connection)
    val fishnetBuildingsNumRDD = FishnetIO.readFishnetDataFromDB("buildings", spark, connection)
    val fishnetOceanRDD = FishnetIO.readFishnetDataFromDB("ocean", spark, connection)
    val fishnetRDD = new GeoFeature(fishnetLandusagesAreaRDD, fishnetWaterareasAreaRDD, fishnetRoadsLengthRDD,
      fishnetAerowaysLengthRDD, fishnetBuildingsNumRDD, fishnetOceanRDD)

   // Get all available reporting areas
    val reportingAreas = airnowRDD.map(_.reportingArea).distinct().collect().sorted
    val trainingFeatureVector = geoFeature.geoFeatureVectorConstruction(reportingAreas, featureName)

    val featureImportance =
      FeatureImportance.calFeatureImportance(trainingFeatureVector, aqiClusterResultRDD, spark)
    val trainingGeoContextRDD = GeoContext.getGeoContext(trainingFeatureVector, featureImportance, spark)
    println("Training data prepared finished")

    val fishnetGeoContextRDD =
      FishnetConstruction.prepareFishnetData(fishnetGidRDD, fishnetRDD, featureName, featureImportance, spark)
    println("Fishnet data prepared finished")

    println("Begin prediction")
    Prediction.fishnetPrediction(airnowRDD, fishnetGidRDD, trainingGeoContextRDD, fishnetGeoContextRDD, spark)
    println("Finished")
  }
}
