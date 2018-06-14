package Utils

import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{PCA, StandardScaler, VectorAssembler}
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.ml.regression.{GBTRegressor, GBTRegressionModel}

import org.apache.spark.sql.DataFrame

object MLUtils {

  def vectorAssembler(df: DataFrame, cols: Array[String], ouputCol: String): DataFrame = {

    val assembler = new VectorAssembler()
      .setInputCols(cols)
      .setOutputCol(ouputCol)

    assembler.transform(df.na.fill(0.0))
  }

  def pca(df: DataFrame, n: Int, inputCol: String, outputCol: String): DataFrame = {

    val pca = new PCA()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setK(n)
      .fit(df)

    pca.transform(df)
  }

  def gbtRegressor(df: DataFrame, featureCol: String, labelCol: String, outputCol: String): GBTRegressionModel = {

    val rf = new GBTRegressor()
      .setFeaturesCol(featureCol)
      .setLabelCol(labelCol)
      .setPredictionCol(outputCol)
      .setMaxIter(100)
      .setMaxDepth(5)

    rf.fit(df)
  }

  def randomForestRegressor(df: DataFrame, featureCol: String, labelCol: String, outputCol: String): RandomForestRegressionModel = {

    val rf = new RandomForestRegressor()
      .setFeaturesCol(featureCol)
      .setLabelCol(labelCol)
      .setPredictionCol(outputCol)
      .setNumTrees(100)
      .setMaxDepth(5)

    rf.fit(df)
  }

  def randomForestClassifier(df: DataFrame, featureCol: String, labelCol: String, outputCol: String): RandomForestClassificationModel = {

    val rf = new RandomForestClassifier()
      .setFeaturesCol(featureCol)
      .setLabelCol(labelCol)
      .setPredictionCol(outputCol)
      .setNumTrees(100) // Tune different parameters
      .setMaxDepth(5)

    rf.fit(df)
  }

  def standardScaler(df: DataFrame, inputCol: String, outputCol: String): DataFrame = {

    val scaler = new StandardScaler()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setWithStd(true)
      .setWithMean(true)

    val scalerModel = scaler.fit(df)
    scalerModel.transform(df)
  }

  def k_means(df: DataFrame, k: Int, iter: Int, featureCol: String, outputCol: String): DataFrame = {

    val km = new KMeans()
      .setK(k)
      .setMaxIter(iter)
      .setFeaturesCol(featureCol)
      .setPredictionCol(outputCol)

    val model = km.fit(df)
    val clusterRes = model.transform(df)
    val WSSSE = model.computeCost(df)
    println(s"When cluster number = $k, within Set Sum of Squared Errors = $WSSSE")
    clusterRes
  }
}
