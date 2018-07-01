package Demo

import java.sql.Timestamp

import MLlib.Evaluation
import Modeling.{FeatureExtraction, GeoFeatureConstruction, Prediction, TimeSeriesPreprocessing,SQLQuery}
import Utils.{Consts, DBConnectionPostgres}
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
    var resultTable = ""
    var errTable = ""

    val predictionColumn = config("prediction_column").asInstanceOf[String]

    val airQualityData = DBConnectionPostgres.dbReadData(airQualityTableName, airQualityColumnSet, conditions, sparkSession)

    val airQualityCleaned = TimeSeriesPreprocessing.dataCleaning(airQualityData, airQualityColumnSet, config).cache()

    val airQualityTimeSeries = TimeSeriesPreprocessing.timeSeriesConstruction(airQualityCleaned, airQualityColumnSet, config, sparkSession).cache()

    val stations = airQualityTimeSeries.rdd.map(x => x.getAs[String](airQualityColumnSet.head)).distinct().collect().toList

    val sensorGeoFeatures = GeoFeatureConstruction.getGeoFeature(Consts.airnow_reporting_area_geofeature_tablename, config, sparkSession)

    val featureName = GeoFeatureConstruction.getFeatureNames(sensorGeoFeatures, config)

    //val geoAbstraction = GeoFeatureConstruction.getGeoAbstraction(stations, sensorGeoFeatures, featureName, config, sparkSession).cache()

    val geoAbstraction = GeoFeatureConstruction.getGeoAbstractionSpeedUp(stations, sensorGeoFeatures, featureName, config, sparkSession).cache()//speedUp

    val channelPair = DBConnectionPostgres.dbReadData("others.purpleair_abchannel" ,List("pid" ,"sensor_id") , "", sparkSession)

    var validationId = List[String]()

    var unfilterOption = config("unfilter_options").asInstanceOf[String]

    var filterState = config("filter").asInstanceOf[Boolean]

    var validationData = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row],new StructType())

    if(!filterState&&unfilterOption=="min"||unfilterOption=="max"||unfilterOption=="avg"){
      val validate = DBConnectionPostgres.dbReadData(validationTableName, validationColumnSet, "", sparkSession)
      validationData = SQLQuery.SQLQuery(validate,channelPair,unfilterOption,config,sparkSession).cache()
    }
    else{
      validationData = DBConnectionPostgres.dbReadData(validationTableName, validationColumnSet, "", sparkSession).cache()
    }

    if(!filterState) {

      if(unfilterOption=="all") {
        resultTable = config("time_to_time_unfilter_table").asInstanceOf[List[String]](0)
        errTable = config("time_to_time_unfilter_table").asInstanceOf[List[String]](1)

        validationId = validationData.rdd.map(x => x.getAs[Int](validationColumnSet.head).toString).distinct().collect().toList
      }
      if(unfilterOption=="min") {
        resultTable = config("time_to_time_unfilter_min").asInstanceOf[List[String]](0)
        errTable = config("time_to_time_unfilter_min").asInstanceOf[List[String]](1)

        validationId = validationData.rdd.map(x => x.getAs[Int](validationColumnSet.head).toString).distinct().collect().toList
      }
      if(unfilterOption=="max") {
        resultTable = config("time_to_time_unfilter_max").asInstanceOf[List[String]](0)
        errTable = config("time_to_time_unfilter_max").asInstanceOf[List[String]](1)

        validationId = validationData.rdd.map(x => x.getAs[Int](validationColumnSet.head).toString).distinct().collect().toList
      }
      if(unfilterOption=="avg") {
        resultTable = config("time_to_time_unfilter_avg").asInstanceOf[List[String]](0)
        errTable = config("time_to_time_unfilter_avg").asInstanceOf[List[String]](1)

        validationId = validationData.rdd.map(x => x.getAs[Int](validationColumnSet.head).toString).distinct().collect().toList
      }
      if(unfilterOption=="a") {
        resultTable = config("time_to_time_unfilter_channel_a").asInstanceOf[List[String]](0)
        errTable = config("time_to_time_unfilter_channel_a").asInstanceOf[List[String]](1)

        validationId = DBConnectionPostgres.dbReadData("(select distinct sensor_id from others.purpleair_los_angeles_channel_a) as a", sparkSession)
          .collect().map(_.getInt(0).toString).toList
      }
      if(unfilterOption=="b") {
        resultTable = config("time_to_time_unfilter_channel_b").asInstanceOf[List[String]](0)
        errTable = config("time_to_time_unfilter_channel_b").asInstanceOf[List[String]](1)

        validationId = DBConnectionPostgres.dbReadData("(select distinct sensor_id from others.purpleair_los_angeles_channel_b) as b", sparkSession)
          .collect().map(_.getInt(0).toString).toList
      }
    }
    /*
        validation id with euclidean distance lower than 50
     */
    if(filterState) {

      var channel = ""
      var joinId = ""

      if(config("filtered_channel")=="a"){
        channel = config("filtered_channel_a").asInstanceOf[List[String]](0)
        joinId = config("filtered_channel_a").asInstanceOf[List[String]](1)
        resultTable = config("filtered_channel_a").asInstanceOf[List[String]](2)
        errTable = config("filtered_channel_a").asInstanceOf[List[String]](3)
      }
      if(config("filtered_channel")=="b"){
        channel = config("filtered_channel_b").asInstanceOf[List[String]](0)
        joinId = config("filtered_channel_b").asInstanceOf[List[String]](1)
        resultTable = config("filtered_channel_b").asInstanceOf[List[String]](2)
        errTable = config("filtered_channel_b").asInstanceOf[List[String]](3)
      }


      validationId = DBConnectionPostgres.dbReadData(s"(select distinct sensor_id from others.purpleair_euclidean_distance ed " +
        s"join others.purpleair_los_angeles_channel_$channel cb on ed.id = cb.$joinId where ed.eu_distance <= 50) as ed ", sparkSession)
        .collect().map(_.getInt(0).toString).toList
    }


    /*
        should change to validation geographic features
    */
    val validationGeoFeatures = GeoFeatureConstruction.getGeoFeature(Consts.purpleair_sensor_la_geofeature_tablename, config, sparkSession)

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

      val t1 = System.currentTimeMillis()

      val dt = airQualityCleaned.filter(airQualityCleaned.col(airQualityColumnSet(1)) === maxTimestamp)

      if (dt.count() <= 10)
        return

      val tmpResult = Prediction.predictionRandomForest(dt, trainingContext, testingContext, config)
        .withColumn("timestamp", functions.lit(maxTimestamp))
        .select(testingContextId, "timestamp", predictionColumn)

      val result = tmpResult.join(validationData, config("validate_join_column").asInstanceOf[List[String]])

      val (rmseVal, m) = Evaluation.rmse(result, validationColumnSet(2), predictionColumn)
      val (maeVal, n) = Evaluation.mae(result, validationColumnSet(2), predictionColumn)
      DBConnectionPostgres.dbWriteData(result,"others",config("current_time_table").asInstanceOf[List[String]](1))
      println(rmseVal,m,maeVal,n)



      val schema = new StructType()
        .add(StructField("timestamp", TimestampType, true))
        .add(StructField("rmse", DoubleType, true))
        .add(StructField("mae", DoubleType, true))
        .add(StructField("sensor_count", IntegerType, true))

      val err = Row(maxTimestamp,rmseVal,maeVal,n)

      val errDF = sparkSession.createDataFrame(
        sparkSession.sparkContext.parallelize(Seq(err)),
        StructType(schema)
      )
      DBConnectionPostgres.dbWriteData(errDF,"others",config("current_time_table").asInstanceOf[List[String]](0))
    }

    if(config("from_time_to_time")==true){

      val times = validationData.select(validationData.col(validationColumnSet(1))).distinct()
        .rdd.map(x => x.getAs[Timestamp](validationColumnSet(1))).collect()

      for (eachTime <- times) {

        val dt = airQualityCleaned.filter(airQualityCleaned.col(airQualityColumnSet(1)) === eachTime)

        if (dt.count() >= 10) {
          val prediction = Prediction.predictionRandomForest(dt, trainingContext, testingContext, config)
            .withColumn("timestamp", functions.lit(eachTime))
            .select(testingContextId, "timestamp", predictionColumn)

          val result = prediction.join(validationData, config("validate_join_column").asInstanceOf[List[String]])

          val (rmseVal, m) = Evaluation.rmse(result, validationColumnSet(2), predictionColumn)
          val (maeVal, n) = Evaluation.mae(result, validationColumnSet(2), predictionColumn)
          DBConnectionPostgres.dbWriteData(result,"others",resultTable)
          println(eachTime,rmseVal,maeVal,n)

          val schema = new StructType()
            .add(StructField("timestamp", TimestampType, true))
            .add(StructField("rmse", DoubleType, true))
            .add(StructField("mae", DoubleType, true))
            .add(StructField("sensor_count", IntegerType, true))

          val err = Row(eachTime,rmseVal,maeVal,n)

          val errDF = sparkSession.createDataFrame(
            sparkSession.sparkContext.parallelize(Seq(err)),
            StructType(schema)
          )
          DBConnectionPostgres.dbWriteData(errDF,"others",errTable)

        }
      }
    }
  }
}
