package databaseIO

import java.sql.Connection

import dataDef.OpenStreetMap
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/*
  Get geographic data for 12 sensors
 */

object GeoIO {

  def readGeoDataFromDB(geoFeature: String,
                        spark: SparkSession,
                        connection: Connection):
  RDD[OpenStreetMap] = {

    val osmArrayBuffer = new ArrayBuffer[OpenStreetMap]()

    // Get geographic features and types
    val statement = connection.createStatement()

    if (geoFeature != "ocean") {
      val resultSet = statement.executeQuery("SELECT * FROM la_geo.la_3000m_buffer_" + geoFeature)

      while (resultSet.next()) {
        val reportingArea = resultSet.getString("reporting_area")
        val featureType = resultSet.getString("feature_type")
        val bufferSize = resultSet.getString("buffer_size").toInt
        val value = resultSet.getString("value").toDouble
        osmArrayBuffer += new OpenStreetMap(reportingArea, geoFeature, featureType, bufferSize, value)
      }

    } else {
      val resultSet = statement.executeQuery(
        "SELECT * FROM la_geo.la_distance_to_ocean;")

      while (resultSet.next()) {
        val reportingArea = resultSet.getString("reporting_area")
        val value = resultSet.getString("distance")
        osmArrayBuffer += new OpenStreetMap(reportingArea, geoFeature, geoFeature, 0, value.toDouble)
      }
    }

    val geoRDD = spark.sparkContext.parallelize(osmArrayBuffer)
    return geoRDD
  }

  // Get geo data at one location [lon, lat]
  def readGeoDataFromDB(lon: Double,
                        lat: Double,
                        geoFeature: String,
                        spark: SparkSession,
                        connection: Connection):
  RDD[OpenStreetMap] = {

    val targetGeoArrayBuffer = new ArrayBuffer[OpenStreetMap]()
    var bufferSize = 100

    var func = "area"
    if (geoFeature == "roads" || geoFeature == "aeroways")  func = "length"

    if (geoFeature != "buildings" && geoFeature != "ocean") {
      while (bufferSize <= 3000) {
        val statement = connection.createStatement()
        val resultSet = statement.executeQuery("SELECT d.type, d.buffer_size, sum(d.value) AS value " +
          "FROM ( SELECT c.type, c.buffer_size, st_" + func + "(geography(st_intersection(c.buffer, c.osm_geom))) AS value " +
          "FROM ( SELECT b.buffer_size, b.buffer, o.type AS type, o.geom AS osm_geom " +
          "FROM (select " + bufferSize + " As buffer_size, geometry(st_buffer(geography(a.geom)," + bufferSize + "::double precision)) AS buffer " +
          "FROM (select ST_GeomFromText('POINT(" + lon + " " + lat + ")',4326) as geom) a) b, " +
          "openstreetmap.los_angeles_california_osm_" + geoFeature + " o " +
          "WHERE st_intersects(b.buffer, o.geom) = true ) c ) d " +
          "GROUP BY d.type, d.buffer_size;")

        while (resultSet.next()) {
          val featureType = resultSet.getString("type")
          val bufferSize = resultSet.getString("buffer_size")
          val value = resultSet.getString("value")
          targetGeoArrayBuffer += new OpenStreetMap("Target Location", geoFeature, featureType, bufferSize.toInt, value.toDouble)
        }
        bufferSize += 100
      }
    } else if (geoFeature == "buildings") {
      while (bufferSize <= 3000) {
        val statement = connection.createStatement()
        val resultSet = statement.executeQuery("SELECT c.type, c.buffer_size, count(*) AS value " +
          "FROM ( SELECT b.buffer_size, b.buffer, o.type AS type, o.geom AS osm_geom " +
          "FROM (select " + bufferSize + " As buffer_size, geometry(st_buffer(geography(a.geom)," + bufferSize + "::double precision)) AS buffer " +
          "FROM (select ST_GeomFromText('POINT(" + lon + " " + lat + ")',4326) as geom) a) b, " +
          "openstreetmap.los_angeles_california_osm_" + geoFeature + " o " +
          "WHERE st_intersects(b.buffer, o.geom) = true ) c " +
          "GROUP BY c.type, c.buffer_size;")

        while (resultSet.next()) {
          val featureType = resultSet.getString("type")
          val bufferSize = resultSet.getString("buffer_size")
          val value = resultSet.getString("value")
          targetGeoArrayBuffer += new OpenStreetMap("Target Location", geoFeature, featureType, bufferSize.toInt, value.toDouble)
        }
        bufferSize += 100
      }
    } else if (geoFeature == "ocean") {
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT st_distance(n.geom, b.geom) AS value " +
        "FROM  (select ST_GeomFromText('POINT(" + lon + " " + lat + ")',4326) as geom) b, ne_10m_ocean n" )
      while (resultSet.next()) {
        val value = resultSet.getString("value")
        targetGeoArrayBuffer += new OpenStreetMap("Target Location", geoFeature, geoFeature, 0, value.toDouble)
      }
    }

    val targetRDD = spark.sparkContext.parallelize(targetGeoArrayBuffer)
    return targetRDD
  }
}