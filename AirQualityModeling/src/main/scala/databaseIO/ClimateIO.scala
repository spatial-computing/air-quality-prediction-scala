package databaseIO

import java.sql.Connection

import airnowdata.Climate
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object ClimateIO {

  def readClimateDataFromDB (timeResolution: String,
                             spark: SparkSession,
                             connection: Connection):
  RDD[Climate] = {

    val climateArrayBuffer = new ArrayBuffer[Climate]()
    val statement = connection.createStatement()

    val resultSetClimate = statement.executeQuery("select * from la_airnow_preprocessing.la_weather_" + timeResolution +
    " where windBearing is not null and reporting_area != 'Pomona Walnut Vly' and reporting_area != 'S San Gabriel Vly' " +
      "and reporting_area != 'S Central LA CO' ")

    while (resultSetClimate.next()) {
      val reportingArea = resultSetClimate.getString("reporting_area")
      val time = resultSetClimate.getString("date_observed")
      val temp = resultSetClimate.getString("temp").toDouble
      val humidity = resultSetClimate.getString("humidity").toDouble
      val windBearing = resultSetClimate.getString("windbearing").toDouble
      val windSpeed = resultSetClimate.getString("windspeed").toDouble
//      val latittude = resultSetClimate.getString("latitude").toDouble
//      val longitude = resultSetClimate.getString("longitude").toDouble
//      val timezone = resultSetClimate.getString("timezone")
//      val precipIntensity = resultSetClimate.getString("precipintensity").toDouble
//      val precipProbability = resultSetClimate.getString("precipprobability").toDouble
//      val pressure = resultSetClimate.getString("pressure").toDouble
//      val icon = resultSetClimate.getString("icon")
//      val dewPoint = resultSetClimate.getString("dewpoint").toDouble
//      val apparentTemp = resultSetClimate.getString("apparenttemp").toDouble
//      val cloudCover = resultSetClimate.getString("cloudcover").toDouble
//      val uvIndex = resultSetClimate.getString("uvIndex").toInt
//      val visibility = resultSetClimate.getString("visibility").toDouble
//      val summary = resultSetClimate.getString("summary")
//      val ozone = resultSetClimate.getString("ozone").toDouble

      val tmp = new Climate(reportingArea, time, temp, humidity, windBearing, windSpeed)

      climateArrayBuffer += tmp
    }

    val climateRDD = spark.sparkContext.parallelize(climateArrayBuffer)
      .filter(x => x.reportingArea != "Antelope Vly")
      .filter(x => x.reportingArea != "E San Gabriel V-1")

    return climateRDD
  }
}
