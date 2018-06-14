package Demo

import java.io.{File, PrintWriter}
import java.sql.Timestamp

import DataSources.{Airnow, GeoFeatures}
import Modeling.{Clustering, ProcUtils, Regression}
import Utils.Consts
import Utils.MLUtils.standardScaler
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object GeoContext_CV {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    val sparkSession = SparkSession
      .builder()
      .appName("AqiPrediction")
      .config("spark.master", "local")
      .getOrCreate()

    val t1 = System.currentTimeMillis()
    val timeResolution = Consts.hourly
    val writer = new PrintWriter(new File(s"/home/yijun/prisms/0418_GeoContext.txt"))
    writer.write("target_area,time,prediction,groundtruth\n")


    val sensorOrFishnet = List("sensor", "sensor_id")

    val airnow = Airnow.dbReadAirnow(timeResolution, sparkSession).cache()
    val geoFeatures = GeoFeatures.geoFeatureConstruction(sensorOrFishnet, sparkSession)
    //    println(s"Total number of available AQI records = ${airnow.count()}")

    val key = "sensor_id"
    val aqiColMap = Map("key"-> "sensor_id", "timestamp" -> "timestamp", "time" -> "unix_time", "val" -> "aqi")
    val geoColMap = Map("key"-> "sensor_id", "feature" -> "feature", "type" -> "feature_type", "size" -> "buffer_size", "val" -> "value")

    val sensors = airnow.rdd.map(x => x.getAs[String](key)).distinct().collect()

    val featureName = GeoFeatures.getFeatureNames(geoFeatures, geoColMap, sparkSession)
    println(System.currentTimeMillis() - t1)

    val geoAbs = ProcUtils.getGeoAbs(sensors, geoFeatures, geoColMap, featureName, sparkSession).cache()
    println(System.currentTimeMillis() - t1)

    val schema1 = new StructType()
      .add(StructField("sensor_id", StringType, true))
      .add(StructField("timestamp", TimestampType, true))
      .add(StructField("aqi", DoubleType, true))
      .add(StructField("prediction", DoubleType, true))
      .add(StructField("error", DoubleType, true))
      .add(StructField("sqrError", DoubleType, true))
    var res = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema1)

    val schema2 = new StructType()
      .add(StructField("sensor_id", StringType, true))
      .add(StructField("timestamp", TimestampType, true))
      .add(StructField("aqi", DoubleType, true))
      .add(StructField("prediction", DoubleType, true))

    for (target <- sensors) {

      val k = Consts.kHourlyMap(target)

      val trainingAirnow = airnow.filter(airnow.col(aqiColMap("key")) =!= target)
      val trainingAbs = geoAbs.filter(geoAbs.col(geoColMap("key")) =!= target)
      val clusterRes = Clustering.kmeansClustering(trainingAirnow, Consts.iter, k, aqiColMap, "cluster", sparkSession)

      val featureImportance = Regression.getFeatureImportance(trainingAbs, clusterRes, aqiColMap("key"), "features", "cluster", "prediction")
      val geoCon = ProcUtils.getGeoCon(geoAbs, featureImportance, key, "features", sparkSession)
//      val pcaGeoCon = ProcUtils.reduceDimension(geoCon, 5, key, "features", sparkSession)

      val all = airnow.join(geoCon, key)
      val scaledAll = standardScaler(all, "features", "scaledFeatures")

      val trainingDf = scaledAll.filter(scaledAll.col(key) =!= target).cache()
      val testingDf = scaledAll.filter(scaledAll.col(key) === target).cache()

      val distinctTime = testingDf.select("unix_time").distinct().rdd.map(x => x.getAs[Long]("unix_time")).collect()

//      val bc = sparkSession.sparkContext.broadcast(trainingDf)

//      val ress = distinctTime.map(x => {
//        val train = bc.value.filter(bc.value.col("unix_time") === x)
//        if (train.count() == 0) null
//        else 1
//      })

      var resSub = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema2)

      for (eachTime <- distinctTime) {
        val train = trainingDf.filter(trainingDf.col("unix_time") === eachTime)
        val test = testingDf.filter(testingDf.col("unix_time") === eachTime)
        if (train.count() > 0) {
          val prediction = Regression.predict(train, test, "features", "aqi", "prediction")
            .select(key, "timestamp", "aqi", "prediction")
//          resSub = resSub.union(prediction)

          val predictionCollect = prediction.collect()(0)
          val aqiVal = predictionCollect.getAs[Double]("aqi")
          val timeVal = predictionCollect.getAs[Timestamp]("timestamp")
          val predictionVal = predictionCollect.getAs[Double]("prediction")
          writer.write(s"$target,$timeVal,$predictionVal,$aqiVal\n")
        }
      }
//      val predictionRes = Regression.predict(trainingDf, testingDf, "new_features", "aqi", "prediction")

//      var errorGeoCon = resSub.select(key, "timestamp", "aqi", "prediction")
//      errorGeoCon = errorGeoCon.withColumn("error", functions.abs(errorGeoCon.col("aqi") - errorGeoCon.col("prediction")))
//      errorGeoCon = errorGeoCon.withColumn("sqrError", errorGeoCon.col("error") * errorGeoCon.col("error"))
//
//      val RMSE_GeoCon = math.sqrt(errorGeoCon.agg(functions.sum(errorGeoCon.col("sqrError")).as("sumError")).collect()(0).getAs[Double]("sumError") / errorGeoCon.count)
//      val MAE_GeoCon = errorGeoCon.agg(functions.sum(errorGeoCon.col("error")).as("sumError")).collect()(0).getAs[Double]("sumError") / errorGeoCon.count
//
//      println(s"Target Station = $target, RMSE = $RMSE_GeoCon, MAE = $MAE_GeoCon")
//
//      res = res.union(errorGeoCon)
    }
//    CSVIO.CSVWriter(res, s"/Users/yijunlin/Research/PRISMS/AirQualityPredictionResult/${System.currentTimeMillis() / 1000}_${timeResolution}_GeoContext")
    writer.close()
    println(s"Finish the program in ${System.currentTimeMillis() - t1}")
  }

}
