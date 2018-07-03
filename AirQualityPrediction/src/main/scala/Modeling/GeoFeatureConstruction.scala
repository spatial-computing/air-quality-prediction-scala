package Modeling

import Utils.DBConnectionPostgres
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.types._

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

object GeoFeatureConstruction {

  val geoFeatureColumnName = "geo_feature_type"

  def getGeoFeatureVector(stations: List[String], geoFeatureData: DataFrame, featureName: Array[String],
                          config: Map[String, Any], sparkSession: SparkSession):
  DataFrame = {

    /*
        Get either geographic abstraction or geo-context based on feature names
     */

    val geoFeatureColumnSet = config("geo_feature_column_set").asInstanceOf[List[String]]
    val idColumnName = geoFeatureColumnSet.head
    val valueColumnName = geoFeatureColumnSet(4)

    val geoFeatureMap = geoFeatureData.rdd.map(x => ((x.getAs[String](idColumnName),
      x.getAs[String](geoFeatureColumnName)),
      x.getAs[Double](valueColumnName))).collectAsMap()

    var geoFeatureVector = new ArrayBuffer[(String, Array[Double])]
    for (station <- stations) {
      val tmp = new ArrayBuffer[Double]()
      for (eachFeature <- featureName) {
        if (geoFeatureMap.contains((station, eachFeature)))
          tmp += geoFeatureMap((station, eachFeature))
        //if this sensor contains this kind of feature if yes return val if not return 0.0
        else
          tmp += 0.0
      }
      geoFeatureVector += ((station, tmp.toArray))
    }

    val geoFeatureRdd = sparkSession.sparkContext.parallelize(geoFeatureVector)
    sparkSession.createDataFrame(geoFeatureRdd.map(x => (x._1, Vectors.dense(x._2)))).toDF(idColumnName, geoFeatureColumnName)
  }

  def getGeoFeatureName(geoFeatureData: DataFrame,
                        config: Map[String, Any]):
  Array[String] = {

    val geoFeatureColumnSet = config("geo_feature_column_set").asInstanceOf[List[String]]
    val idColumnName = geoFeatureColumnSet.head
    val valueColumnName = geoFeatureColumnSet(4)

    val distinctFeatures = geoFeatureData.drop(idColumnName, valueColumnName)
    val distinctFeaturesRdd = distinctFeatures.distinct().rdd.map(x => x.getAs[String](geoFeatureColumnName))

    distinctFeaturesRdd.collect()
  }


  def getGeoFeature(geoFeatureTableName: Map[String, String],
                    config: Map[String, Any], sparkSession: SparkSession):
  DataFrame = {

    var geoFeatureSet = config("geo_feature_set").asInstanceOf[List[String]]

    val geoFeatureColumnSet = config("geo_feature_column_set").asInstanceOf[List[String]]
    val id = geoFeatureColumnSet.head
    val geoFeature = geoFeatureColumnSet(1)
    val featureType = geoFeatureColumnSet(2)
    val bufferSize =geoFeatureColumnSet(3)
    val value = geoFeatureColumnSet(4)

    /*
      Define an empty data frame schema
     */
    val schema = new StructType()
      .add(StructField(id, StringType))
      .add(StructField(geoFeature, StringType))
      .add(StructField(featureType, StringType))
      .add(StructField(bufferSize, IntegerType))
      .add(StructField(value, DoubleType))

    var geoFeatureData = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema)

    // for each geo feature, get the data from database
    for (eachGeoFeature <- geoFeatureSet) {

      val tableName = geoFeatureTableName(eachGeoFeature)

      if (eachGeoFeature == "longitude") {
        val cols = List(s"id as $id", s"'location' as $geoFeature",  s"'longitude' AS $featureType",
          s"0 AS $bufferSize", s"ST_X(location) as $value")
        val df = DBConnectionPostgres.dbReadData(tableName, cols, "", sparkSession)
        geoFeatureData = geoFeatureData.union(df)
      }

      else if (eachGeoFeature == "latitude") {
        val cols = List(s"id as $id", s"'location' as $geoFeature",  s"'latitude' AS $featureType",
          s"0 AS $bufferSize", s"ST_Y(location) as $value")
        val df = DBConnectionPostgres.dbReadData(tableName, cols, "", sparkSession)
        geoFeatureData = geoFeatureData.union(df)
      }

      else if (eachGeoFeature == "ocean") {
        val cols = List(s"id as $id", s"'location' as $geoFeature",  s"'ocean' AS $featureType",
          s"0 AS $bufferSize", s"distance as $value")
        val df = DBConnectionPostgres.dbReadData(tableName, cols, "", sparkSession)
        geoFeatureData = geoFeatureData.union(df)
      }

      else if (eachGeoFeature == "elevation") {
        val cols = List(s"id as $id", s"'location' as $geoFeature",  s"'elevation' AS $featureType",
          s"0 AS $bufferSize", s"elevation as $value")
        val df = DBConnectionPostgres.dbReadData(tableName, cols, "", sparkSession)
        geoFeatureData = geoFeatureData.union(df)
      }

      else {
        val df = DBConnectionPostgres.dbReadData(tableName, geoFeatureColumnSet, "", sparkSession)
        geoFeatureData = geoFeatureData.union(df)
      }
    }

    geoFeatureData = geoFeatureData.withColumn(geoFeatureColumnName, functions.concat_ws("_", geoFeatureData.col(geoFeature),
      geoFeatureData.col(featureType), geoFeatureData.col(bufferSize)))
      .select(id, geoFeatureColumnName, value)

    geoFeatureData
  }
}
