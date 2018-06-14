package dataDef

import airnowdata.Climate
import modeling.DataPreprocessing.climateClusterHelper
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

class ClimateFeature (_tempClusterResult: RDD[ClusterRes],
                      _humidityClusterResult: RDD[ClusterRes],
                      _windBearingClusterResult: RDD[ClusterRes],
                      _windSpeedClusterResult: RDD[ClusterRes]) extends java.io.Serializable {

  var tempClusterResult = _tempClusterResult
  var humidityClusterResult = _humidityClusterResult
  var windBearingClusterResult = _windBearingClusterResult
  var windSpeedClusterResult = _windSpeedClusterResult
}
