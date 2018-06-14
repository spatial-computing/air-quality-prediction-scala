package demo

import java.sql.Connection

import DataSources.{Airnow, CSVIO, GeoFeatures}
import Utils.Consts
import breeze.numerics.sqrt
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import Utils.Consts
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer
import java.io.{File, PrintWriter}


object IDW_CV {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    val sparkSession = SparkSession
      .builder()
      .appName("IDW")
      .config("spark.master", "local")
      .getOrCreate()

    val timeResolution = Consts.monthly
    val key = "sensor_id"


    val airnow = Airnow.dbReadAirnow(timeResolution, sparkSession).cache()
    val dis = GeoFeatures.dbReadSensorDis(sparkSession).cache()
    val weight = dis.withColumn("weight", functions.lit(1.0) / dis.col("distance"))
    val sensors = airnow.rdd.map(x => x.getAs[String](key)).distinct().collect()

    val schema = new StructType()
      .add(StructField("sensor_id", StringType, true))
      .add(StructField("timestamp", TimestampType, true))
      .add(StructField("aqi", DoubleType, true))
      .add(StructField("prediction", DoubleType, true))
      .add(StructField("error", DoubleType, true))
      .add(StructField("sqrError", DoubleType, true))

    var res = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema)
    for (target <- sensors) {

      val trainingDf = airnow.filter(airnow.col(key) =!= target)
      val testingDf = airnow.filter(airnow.col(key) === target)

      val weightSub = weight.filter(weight.col("sensor_a") === target)
      var all = trainingDf.join(weightSub, trainingDf.col(key) === weightSub.col("sensor_b"))
      all = all.withColumn("weighted_aqi", all.col("aqi") * all.col("weight"))

      var prediction = all.groupBy("timestamp")
        .agg(functions.sum(all.col("weighted_aqi")).as("weighted_aqi_sum"), functions.sum(all.col("weight")).as("weighted_sum"))
      prediction = prediction.withColumn("prediction", prediction.col("weighted_aqi_sum") / prediction.col("weighted_sum"))
      val predictionRes = prediction.join(testingDf, "timestamp")

      var errorGeoCon = predictionRes.select(key, "timestamp", "aqi", "prediction")
      errorGeoCon = errorGeoCon.withColumn("error", functions.abs(errorGeoCon.col("aqi") - errorGeoCon.col("prediction")))
      errorGeoCon = errorGeoCon.withColumn("sqrError", errorGeoCon.col("error") * errorGeoCon.col("error"))

      val RMSE_GeoCon = math.sqrt(errorGeoCon.agg(functions.sum(errorGeoCon.col("sqrError")).as("sumError")).collect()(0).getAs[Double]("sumError") / errorGeoCon.count)
      val MAE_GeoCon = errorGeoCon.agg(functions.sum(errorGeoCon.col("error")).as("sumError")).collect()(0).getAs[Double]("sumError") / errorGeoCon.count

      println(s"Target Station = $target, RMSE = $RMSE_GeoCon, MAE = $MAE_GeoCon")
//      CSVIO.CSVWriter(errorGeoCon, s"/Users/yijunlin/Research/PRISMS/AirQualityPredictionResult/Result0405/GeoContext_Weather_$target")

      res = res.union(errorGeoCon)
    }
    CSVIO.CSVWriter(res, s"/Users/yijunlin/Research/PRISMS/AirQualityPredictionResult/Result0405/IDW")

  }
}
