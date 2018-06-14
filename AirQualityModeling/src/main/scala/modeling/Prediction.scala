package modeling


import java.io.FileWriter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import java.sql.{Connection, DriverManager}

import breeze.numerics.abs
import dataDef.{Airnow, AreaDistance, Time}
import dataIO.PredictionResultIO
import org.apache.spark.ml

import scala.collection.mutable.ArrayBuffer


object Prediction {

  def fishnetPrediction(airnowRDD: RDD[Airnow],
                        fishnetGidRDD: RDD[(String, Double, Double)],
                        trainingGeoContextRDD: RDD[(String, Array[Double])],
                        fishnetGeoContextRDD: RDD[(String, Array[Double])],
                        spark: SparkSession):
  Unit = {

    val airnowMap = airnowRDD.map(x => ((x.reportingArea, x.dateObserved.intTime), x.aqi)).collectAsMap()
    val trainingGeoContextMap = trainingGeoContextRDD.collectAsMap()

    val reportingAreas = airnowRDD.map(x => x.reportingArea).distinct().collect().sorted
    val numArea = reportingAreas.length

    val times = airnowRDD.map(x => (x.dateObserved.intTime, 1)).reduceByKey(_+_)
    val distinctTime = times.filter(_._2 != numArea).map(_._1).collect().sorted

    for (time <- distinctTime) {
      val trainingAreas = airnowRDD.filter(_.dateObserved.intTime == time).map(_.reportingArea).distinct()
      val trainingVec = trainingAreas.map(x => (airnowMap((x, time)), trainingGeoContextMap(x)))
      val predictionResult = predictionHelper(trainingVec, fishnetGeoContextRDD, spark)

      PredictionResultIO.writePredictionResult(time, fishnetGidRDD, predictionResult)
    }
  }

  def targetPrediction(airnowRDD: RDD[Airnow],
                       time: Long,
                       trainingGeoContextRDD: RDD[(String, Array[Double])],
                       targetGeoContextRDD: RDD[(String, Array[Double])],
                       spark: SparkSession):
  RDD[(String, Double)] = {

    val airnowFilteredRDD = airnowRDD.filter(_.dateObserved.intTime == time)

    if (airnowFilteredRDD.count() == 0) {
      println("No such time.")
      return null
    }

    val trainingGeoContextMap = trainingGeoContextRDD.collectAsMap()

    val trainingAreas = airnowFilteredRDD.map(_.reportingArea).distinct()
    val trainingMap = airnowFilteredRDD.map(x => (x.reportingArea, x.aqi)).collectAsMap()
    val trainingVec = trainingAreas.map(x => (trainingMap(x), trainingGeoContextMap(x)))

    val predictionResult = predictionHelper(trainingVec, targetGeoContextRDD, spark)
    return predictionResult
  }

  def predictionHelper(trainingVec: RDD[(Double, Array[Double])],
                       testingVec: RDD[(String, Array[Double])],
                       spark: SparkSession) :
  RDD[(String, Double)] = {

    import spark.implicits._
    val trainingDF = trainingVec.map(x => (x._1, ml.linalg.Vectors.dense(x._2))).toDF("aqi", "features")
    val testingDF = testingVec.map(x => (x._1, ml.linalg.Vectors.dense(x._2)))
      .toDF("label", "features")

    val rf = new ml.regression.RandomForestRegressor()
      .setLabelCol("aqi")
      .setFeaturesCol("features")

    val model = rf.fit(trainingDF)
    val prediction = model.transform(testingDF)

    val result = prediction.select("label", "prediction").rdd
      .map(x => (x.getAs[String]("label"), x.getAs[Double]("prediction")))

    return result
  }


  def predictionWithIDW (trainingAirnowData: RDD[Airnow],
                         areaDistanceFilteredRDD: RDD[AreaDistance],
                         time: Long) :
  Double = {

    val trainingMap = trainingAirnowData.filter(_.dateObserved.intTime == time)
      .map(x => (x.reportingArea, x.aqi))
      .collectAsMap()

    var predictionSum = 0.0
    var weightSum = 0.0
    for (each <- areaDistanceFilteredRDD.collect()) {
      val d = each.distance
      predictionSum += trainingMap(each.reportingAreaB) / d / d
      weightSum += 1 / d / d
    }
    if (weightSum == 0.0) return 0.0
    else return predictionSum / weightSum
  }
}