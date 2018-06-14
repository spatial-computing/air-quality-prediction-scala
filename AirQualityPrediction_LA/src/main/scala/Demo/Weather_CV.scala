package Demo

import DataSources.{Airnow, CSVIO, Weather}
import Modeling.{Clustering, ProcUtils, Regression}
import Utils.Consts
import Utils.MLUtils.{standardScaler, vectorAssembler}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession, functions}

object Weather_CV {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    val sparkSession = SparkSession
      .builder()
      .appName("AqiPrediction")
      .config("spark.master", "local")
      .getOrCreate()

    val t1 = System.currentTimeMillis()
    val timeResolution = Consts.hourly

    val sensorOrFishnet = List("sensor", "sensor_id")

    val airnow = Airnow.dbReadAirnow(timeResolution, sparkSession).cache()
    var weather = Weather.dbReadWeather(timeResolution, sparkSession).cache()
    weather = weather.na.fill(0.0)

    val key = "sensor_id"
    val aqiColMap = Map("key"-> "sensor_id", "timestamp" -> "timestamp", "time" -> "unix_time", "val" -> "aqi")
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

      val k = Consts.kHourlyMap(target)

      val trainingAirnow = airnow.filter(airnow.col(aqiColMap("key")) =!= target)
      val testingAirnow = airnow.filter(airnow.col(aqiColMap("key")) === target)

      val all = airnow.join(weather, Seq(key, "unix_time", "timestamp"))
      val cols = all.drop(key, "timestamp", "unix_time", "aqi").columns
      val assAll = vectorAssembler(all, cols, "new_features")
      val scaledAll = standardScaler(assAll, "new_features", "scaledFeatures")

      val trainingDf = scaledAll.filter(scaledAll.col(key) =!= target)
      val testingDf = scaledAll.filter(scaledAll.col(key) === target)

      val predictionRes = Regression.predict(trainingDf, testingDf, "new_features", "aqi", "prediction")

      var errorGeoCon = predictionRes.select(key, "timestamp", "aqi", "prediction")
      errorGeoCon = errorGeoCon.withColumn("error", functions.abs(errorGeoCon.col("aqi") - errorGeoCon.col("prediction")))
      errorGeoCon = errorGeoCon.withColumn("sqrError", errorGeoCon.col("error") * errorGeoCon.col("error"))

      val RMSE_GeoCon = math.sqrt(errorGeoCon.agg(functions.sum(errorGeoCon.col("sqrError")).as("sumError")).collect()(0).getAs[Double]("sumError") / errorGeoCon.count)
      val MAE_GeoCon = errorGeoCon.agg(functions.sum(errorGeoCon.col("error")).as("sumError")).collect()(0).getAs[Double]("sumError") / errorGeoCon.count

      println(s"Target Station = $target, RMSE = $RMSE_GeoCon, MAE = $MAE_GeoCon")

      res = res.union(errorGeoCon)
    }
    CSVIO.CSVWriter(res, s"/Users/yijunlin/Research/PRISMS/AirQualityPredictionResult/Result0405/Weather")
  }

}
