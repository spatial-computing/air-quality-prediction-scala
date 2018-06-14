package DataSources

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object GeoFeatures {

  def geoFeatureConstruction(sensorOrFishnet: List[String],
                             sparkSession: SparkSession): DataFrame = {

    val landusages = dbReadGeoFeatures("landusages", sensorOrFishnet, sparkSession)
      .withColumn("feature", functions.lit("landusages"))
    val waterareas = dbReadGeoFeatures("waterareas", sensorOrFishnet, sparkSession)
      .withColumn("feature", functions.lit("waterareas"))
    val roads = dbReadGeoFeatures("roads", sensorOrFishnet, sparkSession)
      .withColumn("feature", functions.lit("roads"))
    val aeroways = dbReadGeoFeatures("aeroways", sensorOrFishnet, sparkSession)
      .withColumn("feature", functions.lit("aeroways"))
    val buildings = dbReadGeoFeatures("buildings", sensorOrFishnet, sparkSession)
      .withColumn("feature", functions.lit("buildings"))
    val ocean = dbReadGeoFeatures("ocean", sensorOrFishnet, sparkSession)
      .withColumn("feature", functions.lit("ocean"))

    val vol = List("sensor_id", "feature_type", "buffer_size", "value")


    landusages.union(waterareas).union(roads).union(aeroways).union(buildings).union(ocean)
  }


  def getFeatureNames(geoFeatures: DataFrame,
                      c: Map[String, String],
                      sparkSession: SparkSession): RDD[(String, String, Int)] = {

    val distinctFeatures = geoFeatures.drop(c("key"), c("val"))
    val distinctFeaturesRdd = distinctFeatures.rdd.map(x => (x.getAs[String](c("feature")),
      x.getAs[String](c("type")), x.getAs[Int](c("size")))).distinct()

    distinctFeaturesRdd
  }

  def dbReadGeoFeatures (geoFeature: String,
                         sensorOrFishnet: List[String],
                         sparkSession: SparkSession): DataFrame = {

    val schema = "los_angeles"
    val tableName = s"los_angeles_${sensorOrFishnet.head}_geofeature_$geoFeature"
    val cols = List(sensorOrFishnet(1), "feature_type", "buffer_size", "value") // Can be changed into median
    DBConnection.dbReadData(schema, tableName, cols, "", sparkSession)
  }

  def dbReadSensorDis (sparkSession: SparkSession): DataFrame = {
    val schema = "los_angeles"
    val tableName = s"los_angeles_sensors_distance"
    val cols = List("sensor_a", "sensor_b", "distance")
    DBConnection.dbReadData(schema, tableName, cols, "", sparkSession)
  }
}
