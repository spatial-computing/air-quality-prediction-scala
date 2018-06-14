package databaseIO

import java.sql.{Connection, ResultSet}

import dataDef.{Airnow, AreaDistance}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer


object AirnowIO {

  def readAirnowDataFromDB (timeQuery: String,
                            timeResolution: String,
                            spark: SparkSession,
                            connection: Connection):
  RDD[Airnow] = {

    val airnowArrayBuffer = new ArrayBuffer[Airnow]()
    val statement = connection.createStatement()

    // Get AQI at time = timeQuery
    val city = "los_angeles.los_angeles_pm25_"
//    val city = "salt_lake_city.salt_lake_city_airnow_pm25_"
    val resultSetAqi = statement.executeQuery("select * from " + city + timeResolution +
      " where date_observed = '" + timeQuery + "';")
    val airnowRDD = readAirowDataHelper(resultSetAqi, spark)


    println("Total number of available AQI records :" + airnowRDD.count())
    return airnowRDD
  }


  def readAirowDataFromDB (timeResolution: String,
                           spark: SparkSession,
                           connection: Connection):
  RDD[Airnow] = {

    val airnowArrayBuffer = new ArrayBuffer[Airnow]()
    val statement = connection.createStatement()
    val city = "los_angeles.los_angeles_pm25_"
//    val city = "salt_lake_city.salt_lake_city_airnow_pm25_"
    val resultSetAqi = statement.executeQuery("select * from " + city + timeResolution)

    val airnowRDD = readAirowDataHelper(resultSetAqi, spark)
    return airnowRDD
  }

  def readAreaDisFromDB (spark: SparkSession,
                         connection: Connection):
  RDD[AreaDistance] = {

    val areaDistance = new ArrayBuffer[AreaDistance]()
    val statement = connection.createStatement()
    val resultSetDis = statement.executeQuery("SELECT * FROM la_geo.la_reporting_areas_distance")

    while (resultSetDis.next()) {
      val reportingAreaA = resultSetDis.getString("reporting_area_a")
      val reportingAreaB = resultSetDis.getString("reporting_area_b")
      val distance = resultSetDis.getString("distance")
      areaDistance += new AreaDistance(reportingAreaA, reportingAreaB, distance.toDouble)
    }
    val areaDistanceRDD = spark.sparkContext.parallelize(areaDistance)
    return areaDistanceRDD
  }

  def readAirowDataHelper(resultSetAqi: ResultSet,
                          spark: SparkSession)
  : RDD[Airnow] = {

    val airnowArrayBuffer = new ArrayBuffer[Airnow]()

    while (resultSetAqi.next()) {

      val reportingArea = resultSetAqi.getString("sensor_id")
//      val reportingArea = resultSetAqi.getString("reporting_area")
      val timestamp = resultSetAqi.getString("timestamp")
      val aqi = resultSetAqi.getString("aqi")
      val mAqi = resultSetAqi.getString("mean_aqi") // Can be changed into median

      if (aqi == null && mAqi == null) {
      }
      else if (aqi == null && mAqi != null) {
        airnowArrayBuffer += new Airnow(reportingArea, timestamp, mAqi.toDouble)
      }
      else if (aqi != null) {
        airnowArrayBuffer += new Airnow(reportingArea, timestamp, aqi.toDouble)
      }
    }

    val airnowRDD = spark.sparkContext.parallelize(airnowArrayBuffer)
    return airnowRDD.distinct()
  }
}
