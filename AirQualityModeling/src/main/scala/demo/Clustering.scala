package demo

import java.io.{File, PrintWriter}

import connection._
import dataDef.{Airnow, ClimateFeature, ClusterRes, Time}
import dataIO.ClusterResultIO
import databaseIO.{AirnowIO, ClimateIO}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{ml, mllib}
import org.apache.spark.mllib.clustering.{BisectingKMeans, KMeans, KMeansModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer


object Clustering {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder
      .appName("AqiPrediction")
      .config("spark.master", "local")
      .getOrCreate()

    // Connect to database
    val connection = ConnectDB.connectdb(spark)

    // Get sensor data (reporting area, time, aqi)
    val timeResolution = "monthly"
    val airnowRDD = AirnowIO.readAirowDataFromDB(timeResolution, spark, connection)
    val dataRDD = airnowRDD.map(x => (x.reportingArea, x.dateObserved, x.aqi))

    val reportingAreas = dataRDD.map(_._1).distinct().collect().sorted
//    for(eachArea <- reportingAreas) {
//      println("--------------------------------------start for cluster " + eachArea)
//      val dataRDD1 = dataRDD.filter(_._1 != eachArea)
//      val clusterResult = Clustering.cluster(dataRDD1, "", spark)
//    }

//    val climateRDD = ClimateIO.readClimateDataFromDB(timeResolution, spark, connection)
//    val tempRDD = climateRDD.map(x => (x.reportingArea, x.time, x.temp))
//    val tmp = climateRDD.map(x => (x.reportingArea, x.time, x.humidity))

//    val clusterResult = Clustering.cluster(dataRDD, "", spark)
    val clusterResult = Clustering.cluster(dataRDD, "", 7, spark)

    val fileName = "./data/clusterResult.txt"
    ClusterResultIO.writeClusterResult(fileName, clusterResult)
    var k = 7
        // compute WSSE
    val WSSE = Clustering.cluster(dataRDD,"",spark)
    println("Errors: "+ WSSE)
    println("Length: "+ WSSE.length)
    val len = WSSE.length
    var slopeArray = new Array[Double](len)
    for (i<-1 until len) {
      val height = Math.abs(WSSE(i) - WSSE(i-1))
      val width = Math.abs(i - (i-1))
      val slope = height / width
      slopeArray(i) = slope
    }
    var elbow = 0

    for (i<-1 until slopeArray.length) {
      if (slopeArray(i-1) < slopeArray(i)) {
        elbow = i-1
      }

    }
    if (elbow == 0) { elbow = slopeArray.length}
    println("elbow should be: "+ elbow)

//    var v = 0
//    var res = k
//    for (i<-1 to k) {
//      if (k == 2 * v) {
//        res = k
//      }
//      v = i / 2
//    }
//    println("result:"+ res)
  }


  def cluster(dataRDD : RDD[(String, Time, Double)],
              clusterObj: String,
              spark : SparkSession):
  Unit = {

    val reportingAreas = dataRDD.map(x => x._1).distinct().collect().sorted
    val numArea = reportingAreas.length

    // Get Intersection time
    // Key: ReportingArea; Value: Set(times)
    val times = dataRDD.map(x => (x._2.intTime, 1)).reduceByKey(_+_)
    val distinctTime = times.filter(_._2 == numArea).map(_._1).collect().sorted

//    val reportingAreaMap = dataRDD.map(x => (x._1, x._2.intTime)).groupByKey().collectAsMap()
//    var intersectTime = times.toSet
//    for (eachArea <- reportingAreas) {
//      intersectTime = (reportingAreaMap(eachArea).toSet).intersect(intersectTime)
//    }
//    val distinctTime = intersectTime.toArray.sorted

    // Key: (ReportingArea, time); Value: Aqi
    val aqiMap = dataRDD.map(x => ((x._1, x._2.intTime), x._3)).collectAsMap()

    val preparedData = new ArrayBuffer[ArrayBuffer[Double]]()

    for (eachArea <- reportingAreas) {
      var tmpArrayBuffer = new ArrayBuffer[Double]()
      for (eachTime <- distinctTime) {
        if(aqiMap.contains((eachArea, eachTime))) {
          tmpArrayBuffer += aqiMap((eachArea, eachTime))
        }
      }
      preparedData += tmpArrayBuffer
    }

    // Pick the best k
    var k = 1
    while (k <= numArea) {
      clusterHelper(reportingAreas, preparedData, clusterObj, k, spark)
      k += 1
    }
    println("--------------------------------------end for cluster-------------------------------------------------")
  }

  def cluster(dataRDD : RDD[(String, Time, Double)],
              clusterObj: String,
              k : Int,
              spark : SparkSession):

  RDD[ClusterRes] = {

    val reportingAreas = dataRDD.map(x => x._1).distinct().collect().sorted
    val times = dataRDD.map(x => x._2.intTime).distinct().collect().sorted

    // Get Intersection time
    // Key: ReportingArea; Value: Set(times)
    val areaTimeMap = dataRDD.map(x => (x._1, x._2.intTime)).groupByKey().collectAsMap()
    var intersectTime = times.toSet
    for (eachArea <- reportingAreas) {
      intersectTime = (areaTimeMap(eachArea).toSet).intersect(intersectTime)
    }

    val distinctTime = intersectTime.toArray.sorted

    // Key: (ReportingArea, time); Value: Aqi
    val dataMap = dataRDD.map(x => ((x._1, x._2.intTime), x._3)).collectAsMap()

    val preparedData = new ArrayBuffer[ArrayBuffer[Double]]()

    for (eachArea <- reportingAreas) {
      var tmpArrayBuffer = new ArrayBuffer[Double]()
      for (eachTime <- distinctTime) {
        if(dataMap.contains((eachArea, eachTime))) {
          tmpArrayBuffer += dataMap((eachArea, eachTime))
        }
      }
      preparedData += tmpArrayBuffer
    }

    return clusterHelper(reportingAreas, preparedData, clusterObj, k, spark)
  }


  def clusterHelper(reportingAreas: Array[String],
                    preparedData: ArrayBuffer[ArrayBuffer[Double]],
                    clusterObj: String,
                    k: Int,
                    spark : SparkSession):
  RDD[ClusterRes] = {

    /////////////////////////////////////////////////////////
    //k-means//
    /////////////////////////////////////////////////////////
    val numIterators = 500
    val sc = spark.sparkContext
    val preparedDataRDD = sc.parallelize(preparedData)
    val preparedDataMatrix = preparedDataRDD.map(x => mllib.linalg.Vectors.dense(x.toArray))


    /////////////////////////////////////////////////////////
    //normalize//
    val normalize = new mllib.feature.Normalizer()
    val normalizedMatrix = normalize.transform(preparedDataMatrix)
    ///////////////////////////////////////////////////

    val trainingData = normalizedMatrix

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // use hierarchical clustering to determine the initial points
    val bkm = new BisectingKMeans()
      .setK(k)

    val bkmModel = bkm.run(trainingData)
    val centroid = bkmModel.clusterCenters
    val initialModel = new KMeansModel(centroid)

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // modeling
    val clusters = new KMeans()
      .setK(k)
      .setInitialModel(initialModel)
      .setMaxIterations(numIterators)
      .run(trainingData)

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // prediction
    var clusterResult = new ArrayBuffer[ClusterRes]()
    var i = 0
    for (eachLine <- trainingData.collect()) {
      val clusterIdx = clusters.predict(eachLine)
      val result = new ClusterRes(reportingAreas(i).toString, clusterObj, clusterIdx.toInt)
      clusterResult += result
      i += 1
    }

    val WSSSE = clusters.computeCost(trainingData)
    println(clusterObj + " Cluster number is " + k + " Within Set Sum of Squared Errors = " + WSSSE)

    return spark.sparkContext.parallelize(clusterResult)
  }
}

// monthly 4; daily 6; hourly 7
