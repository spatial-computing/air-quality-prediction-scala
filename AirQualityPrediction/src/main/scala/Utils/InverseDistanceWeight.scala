package Utils

import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import scala.collection.Map

object InverseDistanceWeight {

  def idw(testingDf: DataFrame, df: DataFrame, distanceDf: DataFrame,
          config: Map[String, Any], sparkSession: SparkSession): DataFrame = {

    val weight = distanceDf.withColumn("weight", functions.lit(1.0) / distanceDf.col("distance"))

    val dfId = df.schema.fields(0).name
    val dfTime = df.schema.fields(1).name
    val dfValue = df.schema.fields(2).name

    val testingId = testingDf.schema.fields(0).name
    val testingTime = testingDf.schema.fields(1).name
    val testingValue = testingDf.schema.fields(2).name

    val weightIda = weight.schema.fields(0).name
    val weightIdb = weight.schema.fields(1).name

    val testingIds = testingDf.select(testingId).distinct()
    val testingWeight = weight.join(testingIds, weight.col(weightIda) === testingIds.col(testingId))
      .select(weightIda, weightIdb, "weight")

    var distinctTime = df.groupBy(dfTime).agg(functions.count(df.col(dfId)).as("count"))
    distinctTime = distinctTime.filter(distinctTime.col("count") >= 10)
      .withColumnRenamed(dfTime, "timestamp")

    var weightedDf = df.join(distinctTime, df.col(dfTime) === distinctTime.col("timestamp"))
      .join(testingWeight, df.col(dfId) === testingWeight.col(weightIdb))

    weightedDf = weightedDf.withColumn("weighted_value", weightedDf.col(dfValue) * weightedDf.col("weight"))

    var prediction = weightedDf.groupBy(weightIda, "timestamp").agg(functions.sum(weightedDf.col("weighted_value")).as("weighted_value_sum"),
      functions.sum(weightedDf.col("weight")).as("weight_sum"))

    prediction = prediction.withColumn("prediction", prediction.col("weighted_value_sum") / prediction.col("weight_sum"))

    prediction = prediction.join(testingDf, prediction.col("timestamp") === testingDf.col(testingTime) and prediction.col(weightIda) === testingDf.col(testingId))
        .select(testingId, testingTime, testingValue, "prediction")

    prediction
  }
}
