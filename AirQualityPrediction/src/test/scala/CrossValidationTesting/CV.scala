package CrossValidationTesting

import java.sql.Timestamp
import java.text.SimpleDateFormat

import Computation.FeatureTransforming
import Modeling.{FeatureExtraction, GeoFeatureConstruction, TimeSeriesPreprocessing}
import Utils.{Consts, DBConnectionPostgres}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession, functions}
import org.apache.spark.sql.types._

import scala.collection.Map
import scala.io.Source
import scala.util.parsing.json.JSON

object CV {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkSession = SparkSession
      .builder()
      .appName("spiro")
      .config("spark.master", "local[*]")
      .getOrCreate()
    /*
        Get configuration from json file
     */
    val configFile = "src/data/model/config_yijun.json"
    val lines = Source.fromFile(configFile).mkString.replace("\n", "")
    val config = JSON.parseFull(lines).get.asInstanceOf[Map[String, Any]]
    if (config.isEmpty) {
      println("Fail to parse json file.")
      return
    }

    val airQualityTableName = config("air_quality_table_name").asInstanceOf[String]
    val airQualityColumnSet = config("air_quality_column_set").asInstanceOf[List[String]]
    val conditions = config("air_quality_request_condition").asInstanceOf[String]
    val geoFeatureColumnSet = config("geo_feature_column_set").asInstanceOf[List[String]]

    val fm = new SimpleDateFormat("yyyy-MM-dd hh:00:00.0")
    val startTime = new Timestamp(fm.parse(config("start_time").asInstanceOf[String]).getTime)
    val endTime = new Timestamp(fm.parse(config("end_time").asInstanceOf[String]).getTime)

    val labelColumn = config("label_column").asInstanceOf[String]
    val predictionColumn = config("prediction_column").asInstanceOf[String]

    val airQualityId = airQualityColumnSet.head
    val geoFeatureId = geoFeatureColumnSet.head
    val timeColumn = airQualityColumnSet(1)
    val valueColumn = airQualityColumnSet(2)

    var airQualityData = DBConnectionPostgres.dbReadData(airQualityTableName, airQualityColumnSet, conditions, sparkSession)
    airQualityData = airQualityData.filter(airQualityData.col(timeColumn) >= startTime and airQualityData.col(timeColumn) <= endTime).cache()

    val airQualityCleaned = TimeSeriesPreprocessing.dataCleaning(airQualityData, airQualityColumnSet, config).cache()
    val airQualityTimeSeries = TimeSeriesPreprocessing.timeSeriesConstruction(airQualityCleaned, airQualityColumnSet, config, sparkSession).cache()

    val stations = airQualityCleaned.rdd.map(x => x.getAs[String](airQualityColumnSet.head)).distinct().collect().toList

    val sensorGeoFeatures = GeoFeatureConstruction.getGeoFeature(Consts.airnow_reporting_area_geofeature_tablename, config, sparkSession).cache()

//    val featureName = GeoFeatureConstruction.getFeatureNames(sensorGeoFeatures, config)

//    val temp = sensorGeoFeatures.withColumn("geo_feature_type", functions.concat_ws("_", sensorGeoFeatures.col(geoFeatureColumnSet(1)),
//      sensorGeoFeatures.col(geoFeatureColumnSet(2)), sensorGeoFeatures.col(geoFeatureColumnSet(3))))

    var geo_feature_type = sensorGeoFeatures.withColumn("geo_feature", functions.concat_ws("_", sensorGeoFeatures.col(geoFeatureColumnSet(1)),
      sensorGeoFeatures.col(geoFeatureColumnSet(2)), sensorGeoFeatures.col(geoFeatureColumnSet(3))))

    val t = System.currentTimeMillis()
    var geoAbstraction = geo_feature_type.groupBy(geoFeatureId).pivot("geo_feature").avg(geoFeatureColumnSet(4))
    println(System.currentTimeMillis() - t)

    val columns = geoAbstraction.columns.filter(x => x != geoFeatureId)

    val tmp1 = FeatureTransforming.vectorAssembler(geoAbstraction, columns, "geo_feature").cache()

    println(System.currentTimeMillis() - t)

    val schema = new StructType()
      .add(StructField(airQualityId, StringType))
      .add(StructField("timestamp", TimestampType))
      .add(StructField(predictionColumn, DoubleType))

    //    var rmseTotal = 0.0
    //    var maeTotal = 0.0
    //    var nTotal = 0

    for (target <- stations) {

      val trainingStations = stations.filter(x => x != target)
      val testingStations = stations.filter(x => x == target)

      val trainingAirQuality = airQualityCleaned.filter(airQualityCleaned.col(airQualityId) =!= target).cache()
      /*
          testing data should not be cleaned
       */
      val testingAirQuality = airQualityData.filter(airQualityData.col(airQualityId) === target)
        .select(timeColumn, valueColumn).cache()

      val trainingTimeSeries = airQualityTimeSeries.filter(airQualityTimeSeries.col(airQualityId) =!= target)

      val trainingGeoFeatures = sensorGeoFeatures.filter(sensorGeoFeatures.col(geoFeatureId) =!= target)
      val testingGeoFeatures = sensorGeoFeatures.filter(sensorGeoFeatures.col(geoFeatureId) === target)

      val trainingAbstraction = tmp1.filter(tmp1.col(geoFeatureId) =!= target)


      val k = 10 //Consts.kHourlyMap(target)
      val tsCluster = FeatureExtraction.clustering(trainingTimeSeries, k, config)

      val featrueImportance = FeatureExtraction.getFeatureImportance(trainingAbstraction, tsCluster, config)

      println(System.currentTimeMillis() - t)

      val columns_filtered = columns

//      val featureImportance = FeatureExtraction.getFeatureImportance(geoAbstraction, tsCluster, config)



//      val importantFeatures = FeatureExtraction.getImportantFeature(featureName, featrueImportance)
////
//      val trainingContext = GeoFeatureConstruction.getGeoContext(stations, sensorGeoFeatures, importantFeatures, config, sparkSession).cache()




//      val testingContext = GeoFeatureConstruction.getGeoContext(testingStations, testingGeoFeatures, importantFeatures, config, sparkSession).cache()
//
//      val trainingData = trainingAirQuality.join(trainingContext,
//        trainingAirQuality.col(airQualityId) === trainingContext.col(geoFeatureId))
//        .select(airQualityId, timeColumn, labelColumn, "geo_context").cache()
//
//      /*
//          Only test on the time in testing data set
//       */
//
//      val times = testingAirQuality.filter(testingAirQuality.col(timeColumn) >= startTime and testingAirQuality.col(timeColumn) <= endTime)
//        .select(testingAirQuality.col(timeColumn)).distinct()
//        .rdd.map(x => x.getAs[Timestamp](timeColumn)).collect()
//
//      var tmpResult = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema)
//
//      val test = times.map(x => {
//
//        val df = trainingData.filter(trainingData.col(timeColumn) === x)
//        if (df.count() >= 10) {
//          0.0
//        }
//      })

      println("testing")
    }
  }
}
