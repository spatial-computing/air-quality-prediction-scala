package Utils

import org.apache.spark.ml.feature.{PCA, StandardScaler, VectorAssembler}
import org.apache.spark.sql.DataFrame

object FeatureTransforming {

  def vectorAssembler(df: DataFrame,
                      cols: Array[String], ouputCol: String): DataFrame = {

    val assembler = new VectorAssembler()
      .setInputCols(cols)
      .setOutputCol(ouputCol)

    assembler.transform(df.na.fill(0.0))
  }

  def pca(df: DataFrame, n: Int,
          inputCol: String, outputCol: String): DataFrame = {

    val pca = new PCA()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setK(n)
      .fit(df)

    val variance = pca.explainedVariance
    pca.transform(df)
  }

  def standardScaler(df: DataFrame,
                     inputCol: String, outputCol: String): DataFrame = {

    val scaler = new StandardScaler()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setWithStd(true)
      .setWithMean(true)

    val scalerModel = scaler.fit(df)
    scalerModel.transform(df)
  }
}
