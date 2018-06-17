package DataSources

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}

import scala.collection.Map
import Utils.Consts.airnow_reporting_area_geofeature_tablename
import org.apache.spark.sql.types._

object GeoFeatureData {

  def geoFeatureConstruction(geoFeatureTableNames: Map[String, String],
                             config: Map[String, Any],
                             sparkSession: SparkSession): DataFrame = {

    val geoFeatures = config("Geo_Features").asInstanceOf[List[String]]
    val geoFeaturesCols = config("Geo_Features_Cols").asInstanceOf[List[String]]
    val id = geoFeaturesCols.head
    val geoFeature = geoFeaturesCols(1)
    val featureType = geoFeaturesCols(2)
    val bufferSize =geoFeaturesCols(3)
    val value = geoFeaturesCols(4)

    // Define an empty dataframe
    val schema = new StructType()
      .add(StructField(id, StringType, true))
      .add(StructField(geoFeature, StringType, true))
      .add(StructField(featureType, StringType, true))
      .add(StructField(bufferSize, IntegerType, true))
      .add(StructField(value, DoubleType, true))

    var geoFeatureData = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema)

    for (eachGeoFeature <- geoFeatures) {
      val tableName = geoFeatureTableNames(eachGeoFeature)

      if (eachGeoFeature == "longitude") {
        val cols = List(s"id as $id", s"'location' as $geoFeature",  s"'longitude' AS $featureType",
                     s"0 AS $bufferSize", s"ST_X(location) as $value")
        val df = DBConnection.dbReadData(tableName, cols, "", sparkSession)
        geoFeatureData = geoFeatureData.union(df)
      }

      else if (eachGeoFeature == "latitude") {
        val cols = List(s"id as $id", s"'location' as $geoFeature",  s"'latitude' AS $featureType",
                        s"0 AS $bufferSize", s"ST_Y(location) as $value")
        val df = DBConnection.dbReadData(tableName, cols, "", sparkSession)
        geoFeatureData = geoFeatureData.union(df)
      }

      else if (eachGeoFeature == "ocean") {
        val cols = List(s"id as $id", s"'location' as $geoFeature",  s"'ocean' AS $featureType",
                        s"0 AS $bufferSize", s"distance as $value")
        val df = DBConnection.dbReadData(tableName, cols, "", sparkSession)
        geoFeatureData = geoFeatureData.union(df)
      }

      else if (eachGeoFeature == "elevation") {
        val cols = List(s"id as $id", s"'location' as $geoFeature",  s"'elevation' AS $featureType",
                        s"0 AS $bufferSize", s"elevation as $value")
        val df = DBConnection.dbReadData(tableName, cols, "", sparkSession)
        geoFeatureData = geoFeatureData.union(df)
      }

      else {
        val df = DBConnection.dbReadData(tableName, geoFeaturesCols, "", sparkSession)
        geoFeatureData = geoFeatureData.union(df)
      }
    }
    geoFeatureData
  }
}
