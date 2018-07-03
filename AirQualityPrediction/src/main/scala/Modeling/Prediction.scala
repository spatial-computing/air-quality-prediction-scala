package Modeling

import java.io.{File, PrintWriter}

import Computation.MLlib
import java.sql.Timestamp

import Utils.{DBConnectionMongoDB, DBConnectionPostgres}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, functions}
import org.joda.time.DateTime

import scala.collection.Map

object Prediction {

  val predictionColumnName = "prediction"
  val predictionTimestampColumnName = "timestamp"

  def predictionRandomForest(trainingData: DataFrame, trainingId: String,
                             trainingGeoContext: DataFrame, geoFeatureId: String,
                             testingGeoContext: DataFrame, time: Timestamp,
                             config: Map[String, Any]):
  DataFrame = {

    val numTree = config("rf_regression_tree_num").asInstanceOf[Double].toInt
    val depthTree = config("rf_regression_tree_depth").asInstanceOf[Double].toInt

    val input = trainingData.join(trainingGeoContext, trainingData.col(trainingId) === trainingGeoContext.col(geoFeatureId))

    val model = MLlib.randomForestRegressor(input, GeoFeatureConstruction.geoFeatureColumnName,
      TimeSeriesPreprocessing.aqiLabelColumnName, predictionColumnName, numTree, depthTree)

    val result = model.transform(testingGeoContext)
    result.withColumn(predictionTimestampColumnName, functions.lit(time))
  }


  def predictionGBT(trainingData: DataFrame, trainingId: String,
                    trainingGeoContext: DataFrame, geoFeatureId: String,
                    testingGeoContext: DataFrame, time: Timestamp,
                    config: Map[String, Any]):
  DataFrame = {


    val numTree = config("rf_regression_tree_num").asInstanceOf[Double].toInt
    val depthTree = config("rf_regression_tree_depth").asInstanceOf[Double].toInt

    val input = trainingData.join(trainingGeoContext, trainingData.col(trainingId) === trainingGeoContext.col(geoFeatureId))

    val model = MLlib.gbtRegressor(input, GeoFeatureConstruction.geoFeatureColumnName,
      TimeSeriesPreprocessing.aqiLabelColumnName, predictionColumnName, numTree, depthTree)

    val result = model.transform(testingGeoContext)
    result.withColumn(predictionTimestampColumnName, functions.lit(time))
  }
}