package connection


import java.sql.{Connection, DriverManager}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
object ConnectDB {

  def connectdb(spark: SparkSession): Connection = {

    val driver = "org.postgresql.Driver"
//    val url = "jdbc:postgresql://localhost:5432/prisms"
    val url = "jdbc:postgresql://localhost:11223/prisms"

    val username = "yijun"
    val password = "m\\tC7;cc"
    var connection: Connection = null

    // make the connection
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)

    return connection
  }

  def getTrajectoryGeoData(gid: Int, spark: SparkSession, connection: Connection) :
  (RDD[(String, String, Int, Double)], RDD[(String, String, Int, Double)],
    RDD[(String, String, Int, Double)], RDD[(String, String, Int, Int)],
    RDD[(String, String, Int, Double)], RDD[(String, Double)]) = {

    val landusageArea = new ArrayBuffer[(String, String, Int, Double)]()
    val waterArea = new ArrayBuffer[(String, String, Int, Double)]()
    val roadLength = new ArrayBuffer[(String, String, Int, Double)]()
    val buildingNum = new ArrayBuffer[(String, String, Int, Int)]()
    val aerowaysLength = new ArrayBuffer[(String, String, Int, Double)]()
    val oceanDistance = new ArrayBuffer[(String, Double)]()

    // Get geographic features of langusage
    val statement1 = connection.createStatement()
    val resultSetLandusage = statement1.executeQuery("SELECT * FROM trajectory_100m_3000m_buffer_landusages_area_m_view where gid = " + gid.toString + ";")
    while (resultSetLandusage.next()) {
      val reportingArea = resultSetLandusage.getString("gid")
      val landusageType = resultSetLandusage.getString("landusage_type")
      val bufferSize = resultSetLandusage.getString("buffer_size")
      val area = resultSetLandusage.getString("sum_landusage_area")
      val tmpArray = (reportingArea, landusageType, bufferSize.toInt, area.toDouble)
      landusageArea += tmpArray
    }

    // Get geographic features of waterareas
    val statement2 = connection.createStatement()
    val resultSetWaterarea = statement2.executeQuery("SELECT * FROM trajectory_100m_3000m_buffer_waterareas_area_sum_m_view where gid = " + gid.toString + ";")
    while (resultSetWaterarea.next()) {
      val reportingArea = resultSetWaterarea.getString("gid")
      val waterareaType = resultSetWaterarea.getString("waterarea_type")
      val bufferSize = resultSetWaterarea.getString("buffer_size")
      val area = resultSetWaterarea.getString("sum_waterarea_area")
      val tmpArray = (reportingArea, waterareaType, bufferSize.toInt, area.toDouble)
      waterArea += tmpArray
    }

    // Get geographic features of roads
    val statement3 = connection.createStatement()
    val resultSetRoadLength = statement3.executeQuery("SELECT * FROM trajectory_100m_3000m_buffer_roads_length_m_view where gid = " + gid.toString + ";")
    while (resultSetRoadLength.next()) {
      val reportingArea = resultSetRoadLength.getString("gid")
      val roadType = resultSetRoadLength.getString("road_type")
      val bufferSize = resultSetRoadLength.getString("buffer_size")
      val length = resultSetRoadLength.getString("sum_road_length")
      val tmpArray = (reportingArea, roadType, bufferSize.toInt, length.toDouble)
      roadLength += tmpArray
    }

    // Get geographic features of buildings
    val statement4 = connection.createStatement()
    val resultSetBuildingNum = statement3.executeQuery("SELECT * FROM trajectory_100m_3000m_buffer_buildings_num_m_view where gid = " + gid.toString + ";")
    while (resultSetBuildingNum.next()) {
      val reportingArea = resultSetBuildingNum.getString("gid")
      val buildingType = resultSetBuildingNum.getString("building_type")
      val bufferSize = resultSetBuildingNum.getString("buffer_size")
      val num = resultSetBuildingNum.getString("building_num")
      val tmpArray = (reportingArea, buildingType, bufferSize.toInt, num.toInt)
      buildingNum += tmpArray
    }

    // Get geographic features of aeroways
    val statement5 = connection.createStatement()
    val resultSetAerowaysLength = statement3.executeQuery("SELECT * FROM trajectory_100m_3000m_buffer_aeroways_length_sum_m_view where gid = " + gid.toString + ";")
    while (resultSetAerowaysLength.next()) {
      val reportingArea = resultSetAerowaysLength.getString("gid")
      val aerowaysType = resultSetAerowaysLength.getString("aeroways_type")
      val bufferSize = resultSetAerowaysLength.getString("buffer_size")
      val area = resultSetAerowaysLength.getString("sum_aeroways_length")
      val tmpArray = (reportingArea, aerowaysType, bufferSize.toInt, area.toDouble)
      aerowaysLength += tmpArray
    }

    // Get geographic features of distance to ocean
    val statement7 = connection.createStatement()
    val resultSetOcean = statement7.executeQuery("SELECT * FROM trajectory_distance_to_ocean_m_view where gid = " + gid.toString + ";")
    while (resultSetOcean.next()) {
      val reportingArea = resultSetOcean.getString("gid")
      val dis = resultSetOcean.getString("distance")
      var tmpArray = (reportingArea, dis.toDouble)
      oceanDistance += tmpArray
    }

    val landusageAreaRDD = spark.sparkContext.parallelize(landusageArea)
    val waterAreaRDD = spark.sparkContext.parallelize(waterArea)
    val roadLengthRDD = spark.sparkContext.parallelize(roadLength)
    val buildingNumRDD = spark.sparkContext.parallelize(buildingNum)
    val aerowaysLengthRDD = spark.sparkContext.parallelize(aerowaysLength)
    val oceanDistanceRDD = spark.sparkContext.parallelize(oceanDistance)

    return (landusageAreaRDD, waterAreaRDD, roadLengthRDD, buildingNumRDD, aerowaysLengthRDD, oceanDistanceRDD)
  }

}
