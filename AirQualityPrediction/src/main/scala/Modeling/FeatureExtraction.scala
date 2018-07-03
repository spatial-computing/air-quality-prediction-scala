package Modeling

import Computation.{FeatureTransforming, MLlib}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

object FeatureExtraction {

  val scaledTimeSeriesColumnName = "scaled_time_series"
  val scaledGeoFeatureColumnName = "scaled_geo_feature"
  val clusterResultColumnName = "cluster"

  def clustering(timeSeries: DataFrame, k:Int,
                 config: Map[String, Any]):
  DataFrame = {

    /*
       Clustering on time series data
    */

    val ssTimeSeries = FeatureTransforming.standardScaler(timeSeries, TimeSeriesPreprocessing.timeSeriesColumnName, scaledTimeSeriesColumnName)
    val tsCluster = MLlib.kMeans(ssTimeSeries, scaledTimeSeriesColumnName, clusterResultColumnName, k, 100)
    tsCluster
  }


  def getFeatureImportance(geoAbstraction: DataFrame, tsCluster: DataFrame,
                           config: Map[String, Any]):
  Array[Double] = {

    /*
        Get the importance for the geo features
     */

    val numTree = config("rf_classifier_tree_num").asInstanceOf[Double].toInt
    val depthTree = config("rf_classifier_tree_depth").asInstanceOf[Double].toInt

    val tsClusterId = tsCluster.schema.fields.head.name
    val geoAbstractionId = geoAbstraction.schema.fields.head.name

    val df = geoAbstraction.join(tsCluster, tsCluster.col(tsClusterId) === geoAbstraction.col(geoAbstractionId))
    val ssDf = FeatureTransforming.standardScaler(df, GeoFeatureConstruction.geoFeatureColumnName, scaledGeoFeatureColumnName)
    val model = MLlib.randomForestClassifier(ssDf, scaledGeoFeatureColumnName, clusterResultColumnName, "", numTree, depthTree)

    model.featureImportances.toDense.toArray
  }


  def getImportantFeatures(featureName: Array[String],
                           featureImportance: Array[Double]):
  Array[String] = {

    /*
        Get the important geo features (filter 0.0)
     */

    val importantFeaturesName = new ArrayBuffer[String]()

    for (i <- featureImportance.indices)
      if (featureImportance(i) != 0.0)
        importantFeaturesName += featureName(i)

    importantFeaturesName.toArray
  }


  def getSortedFeatures(featureName: Array[String],
                        featureImportance: Array[Double]):
  Array[(Double, String)] = {

    /*
        Get the important geo features (filter 0.0)
        Sorted by importance
     */

    var importantFeaturesName = new ArrayBuffer[(Double, String)]()

    for (i <- featureImportance.indices)
      if (featureImportance(i) != 0.0)
        importantFeaturesName += ((featureImportance(i), featureName(i)))

    importantFeaturesName = importantFeaturesName.sortWith(_._1 > _._1)
    importantFeaturesName.toArray
  }
}
