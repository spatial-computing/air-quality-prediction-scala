package Demo
import java.io.{File, PrintWriter}
import DataSources.{Airnow, GeoFeatures, FishNetPosition}
import Modeling.{Clustering, ProcUtils, Regression}
import Utils.Consts
import Utils.MLUtils.{randomForestRegressor, standardScaler}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}

object FishNet {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    val sparkSession = SparkSession
      .builder()
      .appName("FishNetAqiPrediction")
      .config("spark.master", "local")
      .getOrCreate()

    val t1 = System.currentTimeMillis()
    val timeResolution = Consts.hourly

    //val timestamp = "2016-11-03 16:00:00-07"
    val timestamp = Airnow.getLatesetTimestamp(sparkSession)
    //make the timestamp into a valid filename
    val outputTimestamp = timestamp.split(" ").mkString("_").split(":").mkString("-")
    val outputPath = Consts.outputFilePath+s"${outputTimestamp}_fishnet.csv"

    //the different column names for EPA sensors and fishnet sensors
    val sensor = List("sensor", "sensor_id")
    val fishnet = List("fishnet", "gid")


    val fishNetPosition = FishNetPosition.fishNetPosition(sparkSession)
    //get the air quality data for all time
    val airnowAll = Airnow.dbReadAirnowFishNet(sparkSession).cache()
    val airnow = airnowAll.filter(airnowAll.col("timestamp")===timestamp)
    val sensorNumber = airnow.collect().length

    if(sensorNumber>5) {
      //construct geofeatures for the EPA sensors
      val geoFeatures = GeoFeatures.geoFeatureConstruction(sensor, sparkSession)
      //construct geofeatures for the fishnet sensors
      val fishNetGeoFeatures = GeoFeatures.geoFeatureConstruction(fishnet, sparkSession)

      val sensorKey = "sensor_id"
      val fishNetKey = "gid"

      val aqiColMap = Map("key" -> "sensor_id", "timestamp" -> "timestamp", "time" -> "unix_time", "val" -> "aqi")
      val geoColMap = Map("key" -> "sensor_id", "feature" -> "feature", "type" -> "feature_type", "size" -> "buffer_size", "val" -> "value")
      val fishNetGeoColMap = Map("key" -> "gid", "feature" -> "feature", "type" -> "feature_type", "size" -> "buffer_size", "val" -> "value") //fishnet

      //x:Row type sensors:12 sensor name in a Array[String]
      val sensors = airnow.rdd.map(x => x.getAs[String](sensorKey)).distinct().collect()

      //fishnet gid
      val fishNetPoints = fishNetGeoFeatures.rdd.map(x => x.getAs[Int](fishNetKey).toString).distinct().collect()

      //featureName : drop sensor_id & value and convert to RRD type
      val featureName = GeoFeatures.getFeatureNames(geoFeatures, geoColMap, sparkSession) //for both

      val geoAbs = ProcUtils.getGeoAbs(sensors, geoFeatures, geoColMap, featureName, sparkSession).cache()

      val k = 8
      val trainingAirnow = airnowAll
      //trainingAbs:12rows in total
      val trainingAbs = geoAbs
      val clusterRes = Clustering.kmeansClustering(trainingAirnow, Consts.iter, k, aqiColMap, "cluster", sparkSession)

      //use clustered group as label to find out feature importance Array[Double]
      val featureImportance = Regression.getFeatureImportance(trainingAbs, clusterRes, aqiColMap("key"), "features", "cluster", "prediction")
      //get the name of important features
      val importantFeatureName = sparkSession.sparkContext.parallelize(ProcUtils.getImportantFeatureName(featureImportance, featureName))
      //get geoCon from geoAbs that value is not zero
      val geoCon = ProcUtils.getGeoCon(geoAbs, featureImportance, aqiColMap("key"), "features", sparkSession)
      //for fishnet we skipped the fishnet abstract construction its too time consuming, we uses the important feature name to generate fishnet geo context
      val fishNetGeoCon = ProcUtils.FNgetGeoCon(fishNetPoints, fishNetGeoFeatures, fishNetGeoColMap, importantFeatureName, sparkSession)

      val all = airnow.join(geoCon, sensorKey)
      val fishNetAll = fishNetPosition.join(fishNetGeoCon, fishNetKey)
      //scale the features vector
      val scaledAll = standardScaler(all, "features", "scaledFeatures")
      val FNscaledAll = standardScaler(fishNetAll, "features", "scaledFeatures")

      //train model
      val trainingDf = scaledAll.cache()
      val testingDf = FNscaledAll.cache()
      val model = randomForestRegressor(trainingDf, "features", "aqi": String, "prediction": String)
      val t2 = System.currentTimeMillis()
      val prediction = model.transform(testingDf).select("gid", "log", "lat", "prediction")

      //output cs file
      val tmpDir = "tempDir"
      prediction.coalesce(1).write.format("com.databricks.spark.csv").option("header", true).save(tmpDir)
      val output = outputPath
      val dir = new File(tmpDir)
      val reg = tmpDir + "/part-00000.*.csv"
      val old = dir.listFiles.filter(_.toPath.toString.matches(reg))(0).toString
      (new File(old)).renameTo(new File(output))
      dir.listFiles.foreach(f => f.delete)
      dir.delete

      println(s"Finish the program in ${System.currentTimeMillis() - t1}")
    }
    else
      println("not enough sensor data")

  }
}
