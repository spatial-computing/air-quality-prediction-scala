package Demo

import java.sql.Timestamp

import MLlib.Evaluation
import Modeling.{SQLQuery, TimeSeriesPreprocessing}
import Utils.{Consts, DBConnectionPostgres, InverseDistanceWeight}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

import scala.collection.Map

object IDWTesting {

  def prediction(config: Map[String, Any], sparkSession: SparkSession): Unit = {

    val airQualityTableName = config("air_quality_table_name").asInstanceOf[String]
    val airQualityColumnSet = config("air_quality_column_set").asInstanceOf[List[String]]
    val conditions = config("air_quality_request_condition").asInstanceOf[String]
    val validationTableName = config("validate_table_name").asInstanceOf[String]
    val validationColumnSet = config("validate_column_set").asInstanceOf[List[String]]


    val distanceTableName = config("distance_table_name").asInstanceOf[String]
    val distanceColumnSet = config("distance_column_set").asInstanceOf[List[String]]

    val airQualityData = DBConnectionPostgres.dbReadData(airQualityTableName, airQualityColumnSet, conditions, sparkSession)
    val airQualityCleaned = TimeSeriesPreprocessing.dataCleaning(airQualityData, airQualityColumnSet, config).cache()

    var (validationData,validationId,resultTable,errTable) = SQLQuery.validationDataAndId(validationTableName,validationColumnSet,config("filter").asInstanceOf[Boolean],config("filter_options").asInstanceOf[String],config,sparkSession)



    if(config("filter").asInstanceOf[Boolean]){
      if(config("filter_options")=="a"){
        resultTable = "purpleair_time_to_time_filtered_result_channel_a_idw"
        errTable = "purpleair_time_to_time_filtered_error_channel_a_idw"
      }
      if(config("filter_options")=="b"){
        resultTable = "purpleair_time_to_time_filtered_result_channel_b_idw"
        errTable = "purpleair_time_to_time_filtered_error_channel_b_idw"
      }

    }
    if(!config("filter").asInstanceOf[Boolean]){
      if(config("filter_options")=="a"){
        resultTable = "purpleair_time_to_time_unfilter_result_a_idw"
        errTable = "purpleair_time_to_time_unfilter_error_a_idw"
      }
      if(config("filter_options")=="b"){
        resultTable = "purpleair_time_to_time_unfilter_result_b_idw"
        errTable = "purpleair_time_to_time_unfilter_error_b_idw"
      }
      if(config("filter_options")=="min"){
        resultTable = "purpleair_time_to_time_unfilter_result_min_idw"
        errTable = "purpleair_time_to_time_unfilter_error_min_idw"
      }
      if(config("filter_options")=="max"){
        resultTable = "purpleair_time_to_time_unfilter_result_max_idw"
        errTable = "purpleair_time_to_time_unfilter_error_max_idw"
      }
      if(config("filter_options")=="avg"){
        resultTable = "purpleair_time_to_time_unfilter_result_avg_idw"
        errTable = "purpleair_time_to_time_unfilter_error_avg_idw"
      }
    }

    val distance = DBConnectionPostgres.dbReadData(distanceTableName, distanceColumnSet, "", sparkSession)

    val times = validationData.select(validationData.col(validationColumnSet(1))).distinct()
      .rdd.map(x => x.getAs[Timestamp](validationColumnSet(1))).collect()

    val trainingAirQuality = airQualityCleaned
    val testingAirQuality = validationData.withColumnRenamed("timestamp","date_observed").cache()
    val validationIDWAll = InverseDistanceWeight.idw(testingAirQuality, trainingAirQuality, distance, config, sparkSession).withColumnRenamed("date_observed","timestamp").cache()


    for (eachTime <- times) {
      /*
          testing data should not be cleaned
       */
      val sensorCount = trainingAirQuality.filter(airQualityCleaned("date_observed") === eachTime).count()

      if (sensorCount >= 10) {
        val result = validationIDWAll.filter(validationIDWAll("timestamp")===eachTime)
        //result = InverseDistanceWeight.idw(testingAirQuality, trainingAirQuality, distance, config, sparkSession).withColumnRenamed("date_observed","timestamp")

        //println(resultTable,errTable)
        DBConnectionPostgres.dbWriteData(result,"others",resultTable)

        val (rmseVal, m) = Evaluation.rmse(result, result.schema.fields(2).name, result.schema.fields(3).name)
        val (maeVal, n) = Evaluation.mae(result, result.schema.fields(2).name, result.schema.fields(3).name)

        println(eachTime, rmseVal, maeVal, m, n)

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
