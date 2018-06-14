package Modeling

import Utils.MLUtils.{gbtRegressor, randomForestClassifier, randomForestRegressor, standardScaler}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Regression {

  def predict(trainingDf: DataFrame, testingDf: DataFrame,
              featureCol: String, labelCol: String, outputCol: String):
  DataFrame = {

    val model = randomForestRegressor(trainingDf, featureCol, labelCol: String, outputCol: String)
    val prediction = model.transform(testingDf)
    prediction
  }

  def predict_gbt(trainingDf: DataFrame, testingDf: DataFrame,
                  featureCol: String, labelCol: String, outputCol: String):
  DataFrame = {

    val model = gbtRegressor(trainingDf, featureCol, labelCol, outputCol)
    val prediction = model.transform(testingDf)
    prediction
  }


  def getFeatureImportance(trainingAbs: DataFrame, clusterRes: DataFrame, key: String,
                           featureCol: String, labelCol: String, outputCol: String):
                            //"features","cluster","prediction"
  Array[Double] = {

    val df = trainingAbs.join(clusterRes, key)
//    val scaledDf = standardScaler(df, featureCol, "scaledFeatures")
    val model = randomForestClassifier(df, featureCol, labelCol, outputCol)
    model.featureImportances.toDense.toArray
  }
}
