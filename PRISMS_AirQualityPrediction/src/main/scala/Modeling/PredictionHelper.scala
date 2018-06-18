package Modeling

import DataSources.GeoFeatureUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.Map

object PredictionHelper {

  def modeling(timeSeries: DataFrame,
               trainingLoc: List[String], testingLoc: List[String],
               geoAbstraction: DataFrame, featureName: RDD[(String, String, Int)],
               trainingGeoFeatures: DataFrame, testingGeoFeatures: DataFrame,
               trainingCols: List[String], testingCols: List[String],
               outputLabelCol: String, outputGeoFeatureCol: String, outputScaledFeatureCol: String,
               config: Map[String, Any], sparkSession: SparkSession):

  (DataFrame, DataFrame) = {

    /*
        Clustering on time series data
     */
    val k = config("Kmeans_K").asInstanceOf[Double].toInt
    val ssTimeSeries = FeatureTransforming.standardScaler(timeSeries, outputLabelCol, outputScaledFeatureCol)
    val tsCluster = SparkML.kMeans(ssTimeSeries, outputScaledFeatureCol, "cluster", k, 100)

    /*
        Get important features
     */
    val featureImportance = getFeatureImportance(geoAbstraction, tsCluster, outputGeoFeatureCol, "cluster", "prediction", config)
    val importantFeaturesName = GeoFeatureUtils.getImportantFeaturesName(featureName, featureImportance)

    /*
        Get geo-context
     */
    val trainingContext = GeoFeatureUtils.getGeoContext(trainingGeoFeatures, trainingCols.head, trainingLoc, importantFeaturesName, config, sparkSession)
    val testingContext = GeoFeatureUtils.getGeoContext(testingGeoFeatures, testingCols.head, testingLoc, importantFeaturesName, config, sparkSession)

    val ssTrainingContext = FeatureTransforming.standardScaler(trainingContext, outputGeoFeatureCol, outputScaledFeatureCol).cache()
    val ssTestingContext = FeatureTransforming.standardScaler(testingContext, outputGeoFeatureCol, outputScaledFeatureCol).cache()

    (ssTrainingContext, ssTestingContext)
  }


  def getFeatureImportance(geoAbstraction: DataFrame, tsCluster: DataFrame,
                           featureCol: String, labelCol: String, outputCol: String,
                           config: Map[String, Any]):
  Array[Double] = {

    val numTree = config("RF_Classifier_Tree_Num").asInstanceOf[Double].toInt
    val depthTree = config("RF_Classifier_Tree_Depth").asInstanceOf[Double].toInt
    val tsClusterId = tsCluster.schema.fields.head.name
    val geoAbstractionId = geoAbstraction.schema.fields.head.name

    val df = geoAbstraction.join(tsCluster, tsCluster.col(tsClusterId) === geoAbstraction.col(geoAbstractionId))
    val model = SparkML.randomForestClassifier(df, featureCol, labelCol, outputCol, numTree, depthTree)
    model.featureImportances.toDense.toArray
  }

  def predictionRF(trainingAirQualityData: DataFrame, trainingGeoContext: DataFrame, testingGeoContext: DataFrame,
                   featureCol: String, labelCol: String, outputCol:String,
                   config: Map[String, Any]): DataFrame = {


    val numTree = config("RF_Regression_Tree_Num").asInstanceOf[Double].toInt
    val depthTree = config("RF_Regression_Tree_Depth").asInstanceOf[Double].toInt
    val trainingAirQualityId = trainingAirQualityData.schema.fields.head.name
    val trainingGeoContextId = trainingGeoContext.schema.fields.head.name

    val df = trainingAirQualityData.join(trainingGeoContext, trainingAirQualityData.col(trainingAirQualityId) === trainingGeoContext.col(trainingGeoContextId))
    val model = SparkML.randomForestRegressor(df, featureCol, labelCol, outputCol, numTree, depthTree)
    model.transform(testingGeoContext)
  }

//  def prediction_gbt(trainingDf: DataFrame, testingDf: DataFrame,
//                    featureCol: String, labelCol: String, outputCol: String):
//  DataFrame = {
//
//    val model = SparkML.gbtRegressor(trainingDf, featureCol, labelCol, outputCol, 100, 5)
//    val prediction = model.transform(testingDf)
//    prediction
//  }
}
