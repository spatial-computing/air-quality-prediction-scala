package Modeling

import MLlib.{FeatureTransforming, SparkML}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

object FeatureExtraction {

  def clustering (timeSeries: DataFrame, k:Int,
                  config: Map[String, Any]): DataFrame = {

    /*
       Clustering on time series data
    */

    val ssTimeSeries = FeatureTransforming.standardScaler(timeSeries, "time_series", "scaled_time_series_feature")
    val tsCluster = SparkML.kMeans(ssTimeSeries, "scaled_time_series_feature", "cluster", k, 100)
    tsCluster
  }


  def getFeatureImportance (geoAbstraction: DataFrame, tsCluster: DataFrame,
                            config: Map[String, Any]):
  Array[Double] = {

    /*
        Get important features
     */

    val numTree = config("rf_classifier_tree_num").asInstanceOf[Double].toInt
    val depthTree = config("rf_classifier_tree_depth").asInstanceOf[Double].toInt

    val tsClusterId = tsCluster.schema.fields.head.name
    val geoAbstractionId = geoAbstraction.schema.fields.head.name

    val df = geoAbstraction.join(tsCluster, tsCluster.col(tsClusterId) === geoAbstraction.col(geoAbstractionId))
    val ssDf = FeatureTransforming.standardScaler(df, "geo_feature", "scaled_geo_feature")
    val model = SparkML.randomForestClassifier(ssDf, "scaled_geo_feature", "cluster", "", numTree, depthTree)
    model.featureImportances.toDense.toArray
  }


  def getImportantFeature(featureName: RDD[(String, String, Int)],
                          featureImportance: Array[Double]):
  Array[(String, String, Int)] = {

    val featureCollected = featureName.collect()
    val importantFeaturesName = new ArrayBuffer[((String, String, Int))]()

    for (i <- featureImportance.indices)
      if (featureImportance(i) != 0.0)
        importantFeaturesName += ((featureCollected(i)._1, featureCollected(i)._2, featureCollected(i)._3))

    importantFeaturesName.toArray
  }
}
