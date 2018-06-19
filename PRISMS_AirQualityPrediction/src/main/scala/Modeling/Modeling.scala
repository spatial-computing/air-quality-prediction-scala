package Modeling

import org.apache.spark.sql.DataFrame

import scala.collection.Map

object Modeling {

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
