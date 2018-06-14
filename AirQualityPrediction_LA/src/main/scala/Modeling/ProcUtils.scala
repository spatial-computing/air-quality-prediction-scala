package Modeling

import Utils.MLUtils.pca
import org.apache.spark.ml
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.sql.Timestamp

import scala.collection.mutable.ArrayBuffer

object ProcUtils {


  def getImportantFeatureName(featureImportance:Array[Double],featureName: RDD[(String, String, Int)]):Array[(String,String,Int)]={
    val featureNameList = featureName.collect()
    val feature = Array[Double]()
    val importantFeatureName = new ArrayBuffer[(String,String,Int)]()
    for (i <- featureImportance.indices)
      if (featureImportance(i) != 0.0) importantFeatureName += featureNameList(i)
    importantFeatureName.toArray
  }


  def getGeoAbs(sensors: Array[String], geoFeatures: DataFrame, geoColMap: Map[String, String],
                featureName: RDD[(String, String, Int)], sparkSession: SparkSession): DataFrame = {

    val geoFeaturesMap = geoFeatures.rdd.map(x => ((x.getAs[String](geoColMap("key")), x.getAs[String](geoColMap("feature")),
      x.getAs[String](geoColMap("type")), x.getAs[Int](geoColMap("size"))), x.getAs[Double](geoColMap("val")))).collectAsMap()
    //featureName: feature_type, buffer_size, feature
    var featureVector = new ArrayBuffer[(String, Array[Double])]
    for (each <- sensors) {

      val tmp = featureName.map{case(feature,feature_type,buffer_size) => {
        //ffeatureName here is all the distinct geo feature e.g. aeroways.runway, roads.rail
        if (geoFeaturesMap.contains((each, feature, feature_type, buffer_size))) geoFeaturesMap((each, feature, feature_type, buffer_size))
        //if this sensor contains this kind of feature if yes return val if not return 0.0
        else 0.0
      }}
      featureVector += ((each, tmp.collect()))
    }

    //featureVector:ArrayBuffer["sensorname",val] the value here is a array of val of zero
    val geoFeaturesRdd = sparkSession.sparkContext.parallelize(featureVector)

    //geoColMap("key"):sensor_id features:a vector of geoAbs
    sparkSession.createDataFrame(geoFeaturesRdd.map(x => (x._1, Vectors.dense(x._2)))).toDF(geoColMap("key"), "features")
  }


  def getGeoCon(geoAbs: DataFrame, featureImportance: Array[Double], key: String, feature: String,
                sparkSession: SparkSession): DataFrame = {

    val geoAbsRdd = geoAbs.rdd.map(x => (x.getAs[String](key), x.getAs[ml.linalg.Vector](feature).toArray))
    val geoAbsCon = geoAbsRdd.map{case(sensor_id,features) => {
      val geoConTmp = new ArrayBuffer[Double]()
      for (i <- featureImportance.indices)
        if (featureImportance(i) != 0.0) geoConTmp += features(i)
      (sensor_id, Vectors.dense(geoConTmp.toArray))
    }}
    sparkSession.createDataFrame(geoAbsCon).toDF(key, feature)
  }


  def FNgetGeoCon(sensors: Array[String], geoFeatures: DataFrame, geoColMap: Map[String, String],
                  featureName: RDD[(String, String, Int)], sparkSession: SparkSession): DataFrame = {
    val featureCollected = featureName.collect()

    val geoFeaturesMap = geoFeatures.rdd.map(x => ((x.getAs[Int](geoColMap("key")), x.getAs[String](geoColMap("feature")),
      x.getAs[String](geoColMap("type")), x.getAs[Int](geoColMap("size"))), x.getAs[Double](geoColMap("val"))))
      .filter{case((key,feature,feature_type,buffer_size),value)=>featureCollected.contains((feature,feature_type,buffer_size))}.collectAsMap()

    var featureVector = new ArrayBuffer[(String, Array[Double])]

    for (each <- sensors) {
      val tmp = new ArrayBuffer[Double]()
      for(eachFeature<-featureCollected){
        if (geoFeaturesMap.contains((each.toInt, eachFeature._1, eachFeature._2, eachFeature._3)))
          tmp += geoFeaturesMap((each.toInt, eachFeature._1, eachFeature._2, eachFeature._3))
        //if this sensor contains this kind of feature if yes return val if not return 0.0
        else
          tmp += 0.0
      }
      featureVector += ((each, tmp.toArray))
    }

    val geoFeaturesRdd = sparkSession.sparkContext.parallelize(featureVector)
    sparkSession.createDataFrame(geoFeaturesRdd.map(x => (x._1, Vectors.dense(x._2)))).toDF(geoColMap("key"), "features")

  }

  def reduceDimension(geoCon: DataFrame, n: Int, key: String, feature: String, sparkSession: SparkSession): DataFrame = {

    var reducedRes = pca(geoCon, n, feature, s"pca$feature")
    reducedRes = reducedRes.select(key, s"pca$feature")
    reducedRes = reducedRes.withColumnRenamed(s"pca$feature", feature)
    reducedRes
  }

}
