package Demo

import java.sql.Timestamp

import MLlib.Evaluation
import Modeling._
import Utils.{Consts, DBConnectionMongoDB, DBConnectionPostgres}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.types._

import scala.collection.Map
object FishnetPredicitonWithPurpleair {
  def prediction(config: Map[String, Any], sparkSession: SparkSession): Unit = {

    val airQualityTableName = config("air_quality_table_name").asInstanceOf[String]
    val airQualityColumnSet = config("air_quality_column_set").asInstanceOf[List[String]]
    val conditions = config("air_quality_request_condition").asInstanceOf[String]

    val purpleairTableName = config("purpleair_table_name").asInstanceOf[String]
    val purpleairColumnSet = config("purpleair_column_set").asInstanceOf[List[String]]
    val purpleairConditions = config("purpleair_request_condition").asInstanceOf[String]

    val fishnetTableName = config("fishnet_table_name").asInstanceOf[String]
    val fishnetColumnSet = config("fishnet_column_set").asInstanceOf[List[String]]

    val predictionColumn = config("prediction_column").asInstanceOf[String]

    val idSensorIdConvert = DBConnectionPostgres.dbReadData(config("unfilter_channel").asInstanceOf[String],List("id","sensor_id","channel"),"", sparkSession)

    var purpleairData = DBConnectionPostgres.dbReadData(purpleairTableName, List("sensor_id","timestamp","aqi"),  purpleairConditions , sparkSession)

    purpleairData = purpleairData.join(idSensorIdConvert,"sensor_id")

    purpleairData = purpleairData.withColumn("idTmp", purpleairData("id").cast(StringType))
      .drop("id")
      .withColumnRenamed("idTmp", "id")

    purpleairData = SQLQuery.SQLQuery(purpleairData,"avg",config,sparkSession)


    val epaData = DBConnectionPostgres.dbReadData(airQualityTableName, airQualityColumnSet, conditions, sparkSession)
      .withColumnRenamed("reporting_area", "id").withColumnRenamed("date_observed", "timestamp")


    val purpleairCleaned = TimeSeriesPreprocessing.dataCleaning(purpleairData, purpleairColumnSet, config).cache()
    val epaCleaned = TimeSeriesPreprocessing.dataCleaning(epaData, purpleairColumnSet, config).cache()
    val purpleairAndEpaCleaned = purpleairCleaned.union(epaCleaned).cache()

    var resultTable = ""

    val purpleairTimeSeries = if(config("with_epa_sensors").asInstanceOf[Boolean]) {
      //adding epa sensors
      resultTable = config("time_to_time_purpleair_epa_fishnet").asInstanceOf[String]

      TimeSeriesTest.timeSeriesConstruction(purpleairAndEpaCleaned, purpleairCleaned, purpleairColumnSet, config, sparkSession)
    }
    else{
      //without epa sensors

      resultTable = config("time_to_time_purpleair_fishnet").asInstanceOf[String]

      TimeSeriesTest.timeSeriesConstruction(purpleairCleaned, purpleairColumnSet, config, sparkSession)
    }
    purpleairTimeSeries.cache()

    val purpleairStations = purpleairTimeSeries.rdd.map(x=>x.getAs[String]("id")).collect().toList

    val sensorGeoFeatures = GeoFeatureConstruction.getGeoFeature(Consts.purpleair_sensor_la_geofeature_tablename, config, sparkSession)
      .union(GeoFeatureConstruction.getGeoFeature(Consts.airnow_reporting_area_geofeature_tablename, config, sparkSession))

    val featureName = GeoFeatureConstruction.getFeatureNames(sensorGeoFeatures, config)

    val geoAbstraction = GeoFeatureConstruction.getGeoAbstractionSpeedUp(purpleairStations, sensorGeoFeatures, featureName, config, sparkSession).cache()

    val fishnetGid = DBConnectionPostgres.dbReadData(fishnetTableName, fishnetColumnSet, "", sparkSession)
      .rdd.map(x => x.getAs[String](fishnetColumnSet.head)).distinct().collect().toList

    val fishnetGeoFeatures = GeoFeatureConstruction.getGeoFeature(Consts.la_fishnet_geofeature_tablename, config, sparkSession)

    val k = config("kmeans_k").asInstanceOf[Double].toInt

    val tsCluster = FeatureExtraction.clustering(purpleairTimeSeries, k, config)

    //java heap space out of memory
    var featureImportance = FeatureExtraction.getFeatureImportance(geoAbstraction, tsCluster, config)

    val featureImportanceAvg = featureImportance.filter(_!=0).sum/featureImportance.filter(_!=0).length
    featureImportance = featureImportance.map{x=>
      if(x>featureImportanceAvg*1.8) x
      else 0.0
    }

    val importantFeatures = FeatureExtraction.getImportantFeature(featureName, featureImportance)


    val trainingContext = GeoFeatureConstruction.getGeoContext(purpleairStations, sensorGeoFeatures, importantFeatures, config, sparkSession)

    val testingContext = GeoFeatureConstruction.getGeoContext(fishnetGid, fishnetGeoFeatures, importantFeatures, config, sparkSession)

    val testingContextId = testingContext.schema.fields.head.name

    /*
        Predict for current time
     */
    if (config("current") == true) {

      val maxTimestamp = DBConnectionPostgres.dbReadData(purpleairTableName, List(s"max(${purpleairColumnSet(1)}) as max_timestamp"), purpleairConditions, sparkSession)
        .rdd.map(x => x.getAs[Timestamp]("max_timestamp")).collect()(0)
      val dt = purpleairCleaned.filter(purpleairCleaned.col(purpleairColumnSet(1)) === maxTimestamp)

      if (dt.count() <= 10)
        return

      val result = Prediction.predictionRandomForest(dt, trainingContext, testingContext, config)
        .withColumn("timestamp", functions.lit(maxTimestamp))
        .select(testingContextId, "timestamp", predictionColumn)


      if (config("write_to_db") == true) {

          DBConnectionPostgres.dbWriteData(result,"others",config("current_time_table").asInstanceOf[List[String]](1))

      }
    }

    if (config("from_time_to_time") == true) {

      val times = purpleairCleaned.select(purpleairCleaned.col(purpleairColumnSet(1))).distinct()
        .rdd.map(x => x.getAs[Timestamp](purpleairColumnSet(1))).collect()


      for (eachTime <- times) {

        val dt = if(config("with_epa_sensors").asInstanceOf[Boolean]) {
          purpleairAndEpaCleaned.filter(purpleairAndEpaCleaned.col(purpleairColumnSet(1)) === eachTime)
        }
        else{
          purpleairCleaned.filter(purpleairCleaned.col(purpleairColumnSet(1)) === eachTime)

        }

        if (dt.count() >= 80) {

          val result = PredictionTest.predictionRandomForest(dt, trainingContext, testingContext, config)
            .withColumn("timestamp", functions.lit(eachTime))
            .select(testingContextId, "timestamp", predictionColumn)

          if (config("write_to_db") == true) {
            println(eachTime)
            DBConnectionPostgres.dbWriteData(result,"others",resultTable)
          }

        }
      }
    }
  }
}
