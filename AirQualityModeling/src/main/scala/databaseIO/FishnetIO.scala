package databaseIO

import java.sql.Connection

import dataDef.OpenStreetMap
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object FishnetIO {

  def readFishnetGidFromDB(spark: SparkSession,
                            connection: Connection):
  RDD[(String, Double, Double)] = {

    val fishnet = new ArrayBuffer[(String, Double, Double)]()
    val statement = connection.createStatement()

    val resultSet = statement.executeQuery("SELECT * FROM fishnet_los_angeles")
    while (resultSet.next()) {
      val gid = resultSet.getString("gid")
      val lon = resultSet.getDouble("lon")
      val lat = resultSet.getDouble("lat")
      val tmp = (gid, lon, lat)
      fishnet += tmp
    }
    val fishnetRDD = spark.sparkContext.parallelize(fishnet)
    return fishnetRDD
  }

  def readFishnetDataFromDB(geoFeature: String,
                            spark: SparkSession,
                            connection: Connection):
  RDD[OpenStreetMap] = {

    val fishnetArrayBuffer = new ArrayBuffer[OpenStreetMap]()

    // Get geographic features and types
    val statement = connection.createStatement()

    if (geoFeature != "ocean") {
      val resultSet = statement.executeQuery("SELECT * FROM la_fishnet.la_fishnet_100m_3000m_buffer_"
        + geoFeature + "_sum_m_view;")

      while (resultSet.next()) {
        val gid = resultSet.getString("gid")
        val featureType = resultSet.getString("feature_type")
        val bufferSize = resultSet.getString("buffer_size")
        val value = resultSet.getString("value")

        val tmp = new OpenStreetMap(gid, geoFeature, featureType, bufferSize.toInt, value.toDouble)

        fishnetArrayBuffer += tmp
      }
    } else {
      val resultSet = statement.executeQuery("SELECT * FROM la_fishnet.la_fishnet_distance_to_ocean_m_view;")

      while (resultSet.next()) {
        val gid = resultSet.getString("gid")
        val value = resultSet.getString("distance")

        val tmp = new OpenStreetMap(gid, geoFeature, geoFeature, 0, value.toDouble)
      }
    }

    val fishnetRDD = spark.sparkContext.parallelize(fishnetArrayBuffer)

    return fishnetRDD
  }
}
