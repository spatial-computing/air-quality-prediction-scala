package Modeling

import Utils.SparkML
import org.apache.spark.sql.DataFrame

import scala.collection.Map

object Prediction {


  def predictionRandomForest(trainingAirQualityData: DataFrame, trainingGeoContext: DataFrame,
                             testingGeoContext: DataFrame, config: Map[String, Any]): DataFrame = {

    val numTree = config("rf_regression_tree_num").asInstanceOf[Double].toInt
    val depthTree = config("rf_regression_tree_depth").asInstanceOf[Double].toInt

    val featureColumn = config("feature_column").asInstanceOf[String]
    val labelColumn = config("label_column").asInstanceOf[String]
    val predictionColumn = config("prediction_column").asInstanceOf[String]

    val trainingAirQualityId = trainingAirQualityData.schema.fields.head.name
    val trainingGeoContextId = trainingGeoContext.schema.fields.head.name

    val df = trainingAirQualityData.join(trainingGeoContext,
      trainingAirQualityData.col(trainingAirQualityId) === trainingGeoContext.col(trainingGeoContextId))

    val model = SparkML.randomForestRegressor(df, featureColumn, labelColumn, predictionColumn, numTree, depthTree)
    model.transform(testingGeoContext)
  }
}

//  def prediction_gbt(trainingDf: DataFrame, testingDf: DataFrame,
//                    featureCol: String, labelCol: String, outputCol: String):
//  DataFrame = {
//
//    val model = SparkML.gbtRegressor(trainingDf, featureCol, labelCol, outputCol, 100, 5)
//    val prediction = model.transform(testingDf)
//    prediction
//  }