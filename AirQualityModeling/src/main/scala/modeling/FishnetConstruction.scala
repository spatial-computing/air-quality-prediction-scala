package modeling

import dataDef.GeoFeature
import dataIO.FeatureImportanceIO.writeFeatureImportance
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import modeling.{FeatureImportance, GeoContext}

import scala.collection.mutable.ArrayBuffer

object FishnetConstruction {

  def prepareFishnetData (gidRDD: RDD[(String, Double, Double)],
                          fishnetRDD: GeoFeature,
                          featureTypeName: ArrayBuffer[(String, Int, String)],
                          featureImportance: Array[Double],
                          spark: SparkSession) :

  RDD[(String, Array[Double])] = {

    // Pre-construct feature vector for monitoring stations
    // featureVec : RDD[(String, Array[Double])]  (reporting_area, geo_abstraction)
    // featureName : Array[(String, Int, String)]
    val gids = gidRDD.map(_._1).collect()
    val geoFeatureVector = fishnetRDD.geoFeatureVectorConstruction(gids, featureTypeName)

    // Compute feature importance
    // featureImportance : Array[Double] (importance)
    val fishnetGeoFeatureVector = geoFeatureVector
    val fishnetGeoContext = GeoContext.getGeoContext(fishnetGeoFeatureVector, featureImportance, spark)

    return fishnetGeoContext
  }
}
