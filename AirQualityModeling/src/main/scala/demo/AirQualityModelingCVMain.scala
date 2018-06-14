package demo

import breeze.numerics.sqrt
import connection.ConnectDB
import modeling.{DataPreprocessing, FeatureImportance, GeoContext, Prediction}
import databaseIO.{AirnowIO, ClimateIO}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import util.Constants
import dataIO.FeatureImportanceIO

object AirQualityModelingCVMain {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .appName("AqiPrediction")
      .config("spark.master", "local")
      .getOrCreate()

    val sc = spark.sparkContext
    val connection = ConnectDB.connectdb(spark)
    val timeResolution = "monthly"

    val airnowRDD = AirnowIO.readAirowDataFromDB(timeResolution, spark, connection).cache()
    //    val climateRDD = ClimateIO.readClimateDataFromDB(timeResolution, spark, connection)
    val geoFeature = DataPreprocessing.geoDataPreparation(connection, spark)
    val areaDistanceRDD = AirnowIO.readAreaDisFromDB(spark, connection).cache()

    val featureName = geoFeature.getFeatureName()
    val updatedFeatureName = DataPreprocessing.updateFeatureName(featureName)
    val reportingAreas = airnowRDD.map(_.reportingArea).distinct().cache()

    var err = 0.0
    var geoAbsErr = 0.0
    var IDWErr = 0.0


    for (targetArea <- reportingAreas.collect().sorted) {

      println("----------------------------- Target Station : " + targetArea + " ------------------------------")
      val k = Constants.kMonthlyMap(targetArea)

      val trainingAirnowRDD = airnowRDD.filter(_.reportingArea != targetArea)

      val testingAirnowRDD = airnowRDD.filter(_.reportingArea == targetArea)
        .map(x => (x.dateObserved, x.aqi))
      val groundTruth = testingAirnowRDD.map(x => (x._1.intTime, x._2)).collectAsMap()


      val trainingReportingArea = reportingAreas.distinct().filter(x => x != targetArea).collect().sorted
      val testingReportingArea = reportingAreas.distinct().filter(x => x == targetArea).collect()

      val dataForClusterRDD = trainingAirnowRDD.map(x => (x.reportingArea, x.dateObserved, x.aqi))
      val aqiClusterResultRDD = Clustering.cluster(dataForClusterRDD, "aqi", k, spark)

      //      val filteredClimateRDD = climateRDD.filter(_.reportingArea != targetArea)
      //      val climateFeature = DataPreprocessing.climateClusterPreparation(filteredClimateRDD, k, spark)
      //      val feature = new Feature(geoFeature, climateFeature)

      val trainingFeatureVector = geoFeature.geoFeatureVectorConstruction(trainingReportingArea, featureName)
      val testingFeatureVector = geoFeature.geoFeatureVectorConstruction(testingReportingArea, featureName)
      val trainingFeatureVectorRDD = sc.parallelize(trainingFeatureVector)
      val testingFeatureVectorRDD = sc.parallelize(testingFeatureVector)
      val areaDistanceFilteredRDD = areaDistanceRDD.filter(x => x.reportingAreaA == targetArea)

      val featureImportance =
        FeatureImportance.calFeatureImportance(trainingFeatureVector, aqiClusterResultRDD, spark)

      //      FeatureImportanceIO.writeFeatureImportance(featureImportance, featureName)

      var errSum = 0.0
      var geoAbsErrSum = 0.0
      var IDWErrSum = 0.0

      var i = 100
      while(i <= 100) {
        errSum = 0.0
        // From 10% - 100%
        //        val partFeatureImportance = FeatureImportance.partImportance(featureName, featureImportance, i)
        val featureImportanceFin = featureImportance

        val trainingGeoContextRDD = GeoContext.getGeoContext(trainingFeatureVector, featureImportanceFin, spark)
        val testingGeoContextRDD = GeoContext.getGeoContext(testingFeatureVector, featureImportanceFin, spark)


        val time = testingAirnowRDD.map(x => x._1.intTime).collect().sorted
        for (eachTime <- time) {
          val truth = groundTruth(eachTime)

          // prediction result by using geo-context
          val predictionResult =
            Prediction.targetPrediction(trainingAirnowRDD, eachTime, trainingGeoContextRDD, testingGeoContextRDD, spark)
              .map(_._2).collect()(0)

          errSum += (predictionResult - truth) * (predictionResult - truth)

          // prediction result by using geo-abstraction
          val geoAbsPredictionResult =
            Prediction.targetPrediction(trainingAirnowRDD, eachTime, trainingFeatureVectorRDD, testingFeatureVectorRDD, spark)
              .map(_._2).collect()(0)

          geoAbsErrSum += (geoAbsPredictionResult - truth) * (geoAbsPredictionResult - truth)

          // Prediction result by using IDW
          val IDWPredictionResult = Prediction.predictionWithIDW(trainingAirnowRDD, areaDistanceFilteredRDD, eachTime)
          IDWErrSum += (IDWPredictionResult - truth) * (IDWPredictionResult - truth)
        }

        print("geoContext: " + sqrt(errSum / time.length) + " ")
        print("geoAbstraction: " + sqrt(geoAbsErrSum / time.length) + " ")
        println("IDW: " + sqrt(IDWErrSum / time.length) + " ")
        i += 5
      }
      err += errSum
      geoAbsErr+= geoAbsErrSum
      IDWErr += IDWErrSum
    }
    println("err:" + err/reportingAreas.count())
    println("geoAbsErr:" + geoAbsErr/reportingAreas.count())
    println("IDWErr:" + IDWErr/reportingAreas.count())
  }
}
