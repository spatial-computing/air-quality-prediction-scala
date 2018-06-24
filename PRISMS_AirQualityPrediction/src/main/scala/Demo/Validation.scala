package Demo

import java.sql.Timestamp

import Modeling.{FeatureExtraction, GeoFeatureConstruction, Prediction, TimeSeriesPreprocessing}
import Utils.{Consts, DBConnectionPostgres, Evaluation}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession, functions}

import scala.collection.Map

object Validation {
  def prediction(config: Map[String, Any], sparkSession: SparkSession): Unit = {

    val airQualityTableName = config("air_quality_table_name").asInstanceOf[String]
    val airQualityColumnSet = config("air_quality_column_set").asInstanceOf[List[String]]
    val conditions = config("air_quality_request_condition").asInstanceOf[String]
    val validationTableName = config("validate_table_name").asInstanceOf[String]
    val validationColumnSet = config("validate_column_set").asInstanceOf[List[String]]

    val predictionColumn = config("prediction_column").asInstanceOf[String]

    val t1 = System.currentTimeMillis()

    val airQualityData = DBConnectionPostgres.dbReadData(airQualityTableName, airQualityColumnSet, conditions, sparkSession)

    val airQualityCleaned = TimeSeriesPreprocessing.dataCleaning(airQualityData, airQualityColumnSet, config).cache()

    val airQualityTimeSeries = TimeSeriesPreprocessing.timeSeriesConstruction(airQualityCleaned, airQualityColumnSet, config, sparkSession).cache()

    val stations = airQualityTimeSeries.rdd.map(x => x.getAs[String](airQualityColumnSet.head)).distinct().collect().toList

    val sensorGeoFeatures = GeoFeatureConstruction.getGeoFeature(Consts.airnow_reporting_area_geofeature_tablename, config, false, sparkSession)

    val featureName = GeoFeatureConstruction.getFeatureNames(sensorGeoFeatures, config)

    //val geoAbstraction = GeoFeatureConstruction.getGeoAbstraction(stations, sensorGeoFeatures, featureName, config, sparkSession).cache()

    val geoAbstraction = GeoFeatureConstruction.getGeoAbstractionSpeedUp(stations, sensorGeoFeatures, featureName, config, sparkSession).cache()//speedUp

    val validationData = DBConnectionPostgres.dbReadData(validationTableName, validationColumnSet, "", sparkSession)

    /*
      all validtion id
    */
    //val validationId = validationData.rdd.map(x => x.getAs[Int](validationColumnSet.head).toString).distinct().collect().toList

    /*
        validation id with euclidean distance lower than 50
     */

    val validationId = DBConnectionPostgres.cleanPurpleairId(sparkSession)

    /*
        should change to validation geographic features
    */
    val validationGeoFeatures = GeoFeatureConstruction.getGeoFeature(Consts.purpleair_sensor_la_geofeature_tablename, config, false, sparkSession)

    val k = config("kmeans_k").asInstanceOf[Double].toInt
    val tsCluster = FeatureExtraction.clustering(airQualityTimeSeries, k, config)
    val featrueImportance = FeatureExtraction.getFeatureImportance(geoAbstraction, tsCluster, config)
    val importantFeatures = FeatureExtraction.getImportantFeature(featureName, featrueImportance)

    val trainingContext = GeoFeatureConstruction.getGeoContext(stations, sensorGeoFeatures, importantFeatures, config, sparkSession)
    val testingContext = GeoFeatureConstruction.getGeoContext(validationId, validationGeoFeatures, importantFeatures, config, sparkSession)

    val testingContextId = testingContext.schema.fields.head.name

    /*
         Only test on the time in testing data set
      */
    if(config("current")==true) {

      val maxTimestamp = DBConnectionPostgres.dbReadData(validationTableName, List(s"max(${validationColumnSet(1)}) as max_timestamp"), "", sparkSession)
        .rdd.map(x => x.getAs[Timestamp]("max_timestamp")).collect()(0)

      val dt = airQualityCleaned.filter(airQualityCleaned.col(airQualityColumnSet(1)) === maxTimestamp)

      if (dt.count() <= 10)
        return

      val tmpResult = Prediction.predictionRandomForest(dt, trainingContext, testingContext, config)
        .withColumn("timestamp", functions.lit(maxTimestamp))
        .select(testingContextId, "timestamp", predictionColumn)

      val result = tmpResult.join(validationData,config("validate_join_column").asInstanceOf[List[String]])
      val (rmseVal, m) = Evaluation.rmse(result, validationColumnSet(2), predictionColumn)
      val (maeVal, n) = Evaluation.mae(result, validationColumnSet(2), predictionColumn)

      println(rmseVal,m,maeVal,n)

    }

    if(config("from_time_to_time")==true){

      val times = validationData.select(validationData.col(validationColumnSet(1))).distinct()
        .rdd.map(x => x.getAs[Timestamp](validationColumnSet(1))).collect()

      val schema = new StructType()
        .add(StructField(validationColumnSet.head, StringType, true))
        .add(StructField("timestamp", TimestampType, true))
        .add(StructField(predictionColumn, DoubleType, true))

      var tmpResult = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema)

      var rmseTotal = 0.0
      var maeTotal = 0.0
      var nTotal = 0

      for (eachTime <- times) {

        val dt = airQualityCleaned.filter(airQualityCleaned.col(airQualityColumnSet(1)) === eachTime)
        if (dt.count() >= 10) {
          val prediction = Prediction.predictionRandomForest(dt, trainingContext, testingContext, config)
            .withColumn("timestamp", functions.lit(eachTime))
            .select(testingContextId, "timestamp", predictionColumn)

          tmpResult = tmpResult.union(prediction)
        }
      }
      val result = tmpResult.join(validationData, config("validate_join_column").asInstanceOf[List[String]])
      val (rmseVal, m) = Evaluation.rmse(result, validationColumnSet(2), predictionColumn)
      val (maeVal, n) = Evaluation.mae(result, validationColumnSet(2), predictionColumn)
      rmseTotal += rmseVal
      maeTotal += maeVal
      nTotal += m

    }


  }
}
