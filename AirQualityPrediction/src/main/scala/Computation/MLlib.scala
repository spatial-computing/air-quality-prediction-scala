package Computation

import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor, RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.sql.DataFrame

object MLlib {

  def kMeans(df: DataFrame,
             featureCol: String, outputCol: String,
             k: Int, maxIter: Int): DataFrame = {

    val km = new KMeans()
      .setFeaturesCol(featureCol)
      .setPredictionCol(outputCol)
      .setK(k)
      .setMaxIter(maxIter)

    val model = km.fit(df)
    val clusterRes = model.transform(df)
    val WSSSE = model.computeCost(df)
    println(s"'When cluster number = $k, within Set Sum of Squared Errors = $WSSSE',")
    clusterRes
  }

  def gbtRegressor(df: DataFrame,
                   featureCol: String, labelCol: String, outputCol: String,
                   maxIter: Int, maxDepth:Int): GBTRegressionModel = {

    val rf = new GBTRegressor()
      .setFeaturesCol(featureCol)
      .setLabelCol(labelCol)
      .setPredictionCol(outputCol)
      .setMaxIter(maxIter)
      .setMaxDepth(maxDepth)

    rf.fit(df)
  }

  def randomForestRegressor(df: DataFrame,
                            featureCol: String, labelCol: String, outputCol: String,
                            numTree: Int, maxDepth: Int): RandomForestRegressionModel = {

    val rf = new RandomForestRegressor()
      .setFeaturesCol(featureCol)
      .setLabelCol(labelCol)
      .setPredictionCol(outputCol)
      .setNumTrees(numTree)
//      .setMaxDepth(maxDepth)

    rf.fit(df)
  }

  def randomForestClassifier(df: DataFrame,
                             featureCol: String, labelCol: String, outputCol: String,
                             numTree: Int, maxDepth: Int): RandomForestClassificationModel = {

    val rf = new RandomForestClassifier()
      .setFeaturesCol(featureCol)
      .setLabelCol(labelCol)
      .setPredictionCol(outputCol)
      .setNumTrees(numTree) // Tune different parameters
//      .setMaxDepth(maxDepth)

    rf.fit(df)
  }

}
