package modeling

import dataDef._
import databaseIO.{AirnowIO, ClimateIO, GeoIO}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import java.sql.Connection
import airnowdata.Climate
import demo.Clustering

import scala.collection.mutable.ArrayBuffer

object DataPreprocessing {

  def geoDataPreparation (connection: Connection,
                          spark: SparkSession) :
  GeoFeature = {

    val landusagesAreaRDD = GeoIO.readGeoDataFromDB("landusages", spark, connection)
    val waterareasAreaRDD = GeoIO.readGeoDataFromDB("waterareas", spark, connection)
    val roadsLengthRDD = GeoIO.readGeoDataFromDB("roads", spark, connection)
    val aerowaysLengthRDD = GeoIO.readGeoDataFromDB("aeroways", spark, connection)
    val buildingsNumRDD = GeoIO.readGeoDataFromDB("buildings", spark, connection)
    val oceanRDD = GeoIO.readGeoDataFromDB("ocean", spark, connection)
    val geoFeatures = new GeoFeature(landusagesAreaRDD, waterareasAreaRDD, roadsLengthRDD,
      aerowaysLengthRDD, buildingsNumRDD, oceanRDD)

    return geoFeatures
  }

  def climateClusterPreparation(climateRDD: RDD[Climate],
                                k: Int,
                                spark: SparkSession):
  ClimateFeature = {

    val sc = spark.sparkContext

    val tempClusterResultRDD = climateClusterHelper(climateRDD, "temp", k, spark)
    val humidityClusterResultRDD = climateClusterHelper(climateRDD, "humidity", k, spark)
    val windBearingClusterResultRDD = climateClusterHelper(climateRDD, "windBearing", k, spark)
    val windSpeedClusterResultRDD = climateClusterHelper(climateRDD, "windSpeed", k, spark)

    return new ClimateFeature(tempClusterResultRDD, humidityClusterResultRDD,
      windBearingClusterResultRDD, windSpeedClusterResultRDD)
  }

  def climateClusterHelper(climateRDD: RDD[Climate],
                           climateType: String,
                           k: Int,
                           spark: SparkSession):

  RDD[ClusterRes] = {

    val clusterResult = new ArrayBuffer[ClusterRes]()
    var clusterResultRDD = spark.sparkContext.parallelize(clusterResult)

    if (climateType == "temp") {
      val tmp = climateRDD.map(x => (x.reportingArea, x.time, x.temp))
      clusterResultRDD = Clustering.cluster(tmp, climateType, k, spark)
    }
    else if (climateType == "humidity") {
      val tmp = climateRDD.map(x => (x.reportingArea, x.time, x.humidity))
      clusterResultRDD = Clustering.cluster(tmp, climateType, k, spark)
    }
    else if (climateType == "windBearing") {
      val tmp = climateRDD.map(x => (x.reportingArea, x.time, x.windBearing))
      clusterResultRDD = Clustering.cluster(tmp, climateType, k, spark)
    }
    else if (climateType == "windSpeed") {
      val tmp = climateRDD.map(x => (x.reportingArea, x.time, x.windSpeed))
      clusterResultRDD = Clustering.cluster(tmp, climateType, k, spark)
    }
    return clusterResultRDD
  }

  def updateFeatureName(featureName: ArrayBuffer[(String, Int, String)])
  :(ArrayBuffer[(String, Int, String)]) = {

    var updatedFeatureName = featureName

    var featureN = ("temp", 0, "degree")
    updatedFeatureName += featureN

    featureN = ("humidity", 0, "degree")
    updatedFeatureName += featureN

    featureN = ("windBearing", 0, "degree")
    updatedFeatureName += featureN

    featureN = ("windSpeed", 0, "degree")
    updatedFeatureName += featureN

    return updatedFeatureName
  }

//  tmpArrayBuffer += climateFeature.tempClusterResult.filter(_.reportingArea == eachArea).collect()(0).cluster
//  tmpArrayBuffer += climateFeature.humidityClusterResult.filter(_.reportingArea == eachArea).collect()(0).cluster
//  tmpArrayBuffer += climateFeature.windBearingClusterResult.filter(_.reportingArea == eachArea).collect()(0).cluster
//  tmpArrayBuffer += climateFeature.windSpeedClusterResult.filter(_.reportingArea == eachArea).collect()(0).cluster
}
