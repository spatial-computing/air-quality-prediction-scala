package Modeling

import Utils.{DBConnectionPostgres}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

object GeoFeatureConstruction {

  def getGeoContext(locations: List[String], geoFeatureData: DataFrame,
                    importantFeatureName: Array[(String, String, Int)],
                    config: Map[String, Any], sparkSession: SparkSession): DataFrame = {

    val geoFeatureColumnSet = config("geo_feature_column_set").asInstanceOf[List[String]]
    val location = geoFeatureColumnSet.head
    val geoFeature = geoFeatureColumnSet(1)
    val featureType = geoFeatureColumnSet(2)
    val bufferSize = geoFeatureColumnSet(3)
    val value = geoFeatureColumnSet(4)

    val geoFeatureMap = geoFeatureData.rdd.map(x => ((x.getAs[String](location), x.getAs[String](geoFeature),
      x.getAs[String](featureType), x.getAs[Int](bufferSize)), x.getAs[Double](value)))
      .filter { case ((k, df, ft, bz), v) => importantFeatureName.contains((df, ft, bz)) }.collectAsMap()

    var featureVector = new ArrayBuffer[(String, Array[Double])]
    for (loc <- locations) {

      val tmp = new ArrayBuffer[Double]()
      for (eachFeature <- importantFeatureName) {
        if (geoFeatureMap.contains((loc, eachFeature._1, eachFeature._2, eachFeature._3)))
          tmp += geoFeatureMap((loc, eachFeature._1, eachFeature._2, eachFeature._3))
        else
          tmp += 0.0 //if this location contains this kind of feature else return 0.0
      }
      featureVector += ((loc, tmp.toArray))
    }

    val geoFeatureRdd = sparkSession.sparkContext.parallelize(featureVector)
    sparkSession.createDataFrame(geoFeatureRdd.map(x => (x._1, Vectors.dense(x._2))))
      .toDF(location, "geo_features")
  }


  def getGeoAbstraction(stations: List[String], geoFeatureData: DataFrame,
                        featureName: RDD[(String, String, Int)],
                        config: Map[String, Any], sparkSession: SparkSession):
  DataFrame = {

    val geoFeatureColumnSet = config("geo_feature_column_set").asInstanceOf[List[String]]
    val location = geoFeatureColumnSet.head
    val geoFeature = geoFeatureColumnSet(1)
    val featureType = geoFeatureColumnSet(2)
    val bufferSize = geoFeatureColumnSet(3)
    val value = geoFeatureColumnSet(4)

    val geoFeatureMap = geoFeatureData.rdd.map(x => ((x.getAs[String](location), x.getAs[String](geoFeature),
      x.getAs[String](featureType), x.getAs[Int](bufferSize)), x.getAs[Double](value))).collectAsMap()

    var geoFeatureVector = new ArrayBuffer[(String, Array[Double])]
    for (station <- stations) {
      val tmp = featureName.map { x => {
        if (geoFeatureMap.contains((station, x._1, x._2, x._3)))
          geoFeatureMap((station, x._1, x._2, x._3))
        else 0.0 //if this sensor contains this kind of feature if yes return val if not return 0.0
       }
      }
      geoFeatureVector += ((station, tmp.collect()))
    }

    val geoFeatureRdd = sparkSession.sparkContext.parallelize(geoFeatureVector)
    sparkSession.createDataFrame(geoFeatureRdd.map(x => (x._1, Vectors.dense(x._2)))).toDF(location, "geo_feature")
  }

  def getGeoAbstractionSpeedUp(stations: List[String], geoFeatureData: DataFrame,
                        featureName: RDD[(String, String, Int)],
                        config: Map[String, Any], sparkSession: SparkSession):
  DataFrame = {

    val geoFeatureColumnSet = config("geo_feature_column_set").asInstanceOf[List[String]]
    val location = geoFeatureColumnSet.head
    val geoFeature = geoFeatureColumnSet(1)
    val featureType = geoFeatureColumnSet(2)
    val bufferSize = geoFeatureColumnSet(3)
    val value = geoFeatureColumnSet(4)

    val featureNameCollected = featureName.collect()

    val geoFeatureMap = geoFeatureData.rdd.map(x => ((x.getAs[String](location), x.getAs[String](geoFeature),
      x.getAs[String](featureType), x.getAs[Int](bufferSize)), x.getAs[Double](value))).collectAsMap()

    var geoFeatureVector = new ArrayBuffer[(String, Array[Double])]

    for (station <- stations) {
      val tmp = new ArrayBuffer[Double]()
      for (eachFeature <- featureNameCollected) {
        if (geoFeatureMap.contains((station, eachFeature._1, eachFeature._2, eachFeature._3)))
          tmp += geoFeatureMap((station, eachFeature._1, eachFeature._2, eachFeature._3))
        //if this sensor contains this kind of feature if yes return val if not return 0.0
        else
          tmp += 0.0
      }
      geoFeatureVector += ((station, tmp.toArray))
    }

    val geoFeatureRdd = sparkSession.sparkContext.parallelize(geoFeatureVector)
    sparkSession.createDataFrame(geoFeatureRdd.map(x => (x._1, Vectors.dense(x._2)))).toDF(location, "geo_feature")
  }


  def getFeatureNames(geoFeatureData: DataFrame,
                      config: Map[String, Any]):
  RDD[(String, String, Int)] = {

    val geoFeatureColumnSet = config("geo_feature_column_set").asInstanceOf[List[String]]
    val location = geoFeatureColumnSet.head
    val geoFeature = geoFeatureColumnSet(1)
    val featureType = geoFeatureColumnSet(2)
    val bufferSize = geoFeatureColumnSet(3)
    val value = geoFeatureColumnSet(4)

    val distinctFeatures = geoFeatureData.drop(location, value)
    val distinctFeaturesRdd = distinctFeatures.distinct()
      .rdd.map(x => (x.getAs[String](geoFeature), x.getAs[String](featureType), x.getAs[Int](bufferSize)))

    distinctFeaturesRdd
  }


  def getGeoFeature (geoFeatureTableName: Map[String, String],
                     config: Map[String, Any],withOrWithOutElevation:Boolean,
                     sparkSession: SparkSession): DataFrame = {
    var geoFeatureSet = List[String]()
    if(withOrWithOutElevation) {
      geoFeatureSet = config("geo_feature_set").asInstanceOf[List[String]]
    }
    else {
      geoFeatureSet = config("geo_feature_set_without_elevation").asInstanceOf[List[String]]
    }

    val geoFeatureColumnSet = config("geo_feature_column_set").asInstanceOf[List[String]]
    val location = geoFeatureColumnSet.head
    val geoFeature = geoFeatureColumnSet(1)
    val featureType = geoFeatureColumnSet(2)
    val bufferSize =geoFeatureColumnSet(3)
    val value = geoFeatureColumnSet(4)

    // Define an empty dataframe
    val schema = new StructType()
      .add(StructField(location, StringType, true))
      .add(StructField(geoFeature, StringType, true))
      .add(StructField(featureType, StringType, true))
      .add(StructField(bufferSize, IntegerType, true))
      .add(StructField(value, DoubleType, true))

    var geoFeatureData = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], schema)

    for (eachGeoFeature <- geoFeatureSet) {
      val tableName = geoFeatureTableName(eachGeoFeature)

      if (eachGeoFeature == "longitude") {
        val cols = List(s"id as $location", s"'location' as $geoFeature",  s"'longitude' AS $featureType",
          s"0 AS $bufferSize", s"ST_X(location) as $value")
        val df = DBConnectionPostgres.dbReadData(tableName, cols, "", sparkSession)
        geoFeatureData = geoFeatureData.union(df)
      }

      else if (eachGeoFeature == "latitude") {
        val cols = List(s"id as $location", s"'location' as $geoFeature",  s"'latitude' AS $featureType",
          s"0 AS $bufferSize", s"ST_Y(location) as $value")
        val df = DBConnectionPostgres.dbReadData(tableName, cols, "", sparkSession)
        geoFeatureData = geoFeatureData.union(df)
      }

      else if (eachGeoFeature == "ocean") {
        val cols = List(s"id as $location", s"'location' as $geoFeature",  s"'ocean' AS $featureType",
          s"0 AS $bufferSize", s"distance as $value")
        val df = DBConnectionPostgres.dbReadData(tableName, cols, "", sparkSession)
        geoFeatureData = geoFeatureData.union(df)
      }

      else if (eachGeoFeature == "elevation") {
        val cols = List(s"id as $location", s"'location' as $geoFeature",  s"'elevation' AS $featureType",
          s"0 AS $bufferSize", s"elevation as $value")
        val df = DBConnectionPostgres.dbReadData(tableName, cols, "", sparkSession)
        geoFeatureData = geoFeatureData.union(df)
      }

      else {
        val df = DBConnectionPostgres.dbReadData(tableName, geoFeatureColumnSet, "", sparkSession)
        geoFeatureData = geoFeatureData.union(df)
      }
    }
    geoFeatureData
  }
}
