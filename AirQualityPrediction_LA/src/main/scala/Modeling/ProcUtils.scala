package Modeling

import Utils.MLUtils.pca
import org.apache.spark.ml
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

object ProcUtils {

  def getGeoAbs(sensors: Array[String], geoFeatures: DataFrame, geoColMap: Map[String, String],
                featureName: RDD[(String, String, Int)], sparkSession: SparkSession): DataFrame = {

    val geoFeaturesMap = geoFeatures.rdd.map(x => ((x.getAs[String](geoColMap("key")), x.getAs[String](geoColMap("feature")),
      x.getAs[String](geoColMap("type")), x.getAs[Int](geoColMap("size"))), x.getAs[Double](geoColMap("val")))).collectAsMap()

    var featureVector = new ArrayBuffer[(String, Array[Double])]
    for (each <- sensors) {
      val tmp = featureName.map(x => {
        if (geoFeaturesMap.contains((each, x._1, x._2, x._3))) geoFeaturesMap((each, x._1, x._2, x._3))
        else 0.0
      })
      featureVector += ((each, tmp.collect()))
    }

    val geoFeaturesRdd = sparkSession.sparkContext.parallelize(featureVector)
    sparkSession.createDataFrame(geoFeaturesRdd.map(x => (x._1, Vectors.dense(x._2)))).toDF(geoColMap("key"), "features")
  }

  def getGeoCon(geoAbs: DataFrame, featureImportance: Array[Double], key: String, feature: String,
                sparkSession: SparkSession): DataFrame = {

    val geoAbsRdd = geoAbs.rdd.map(x => (x.getAs[String](key), x.getAs[ml.linalg.Vector](feature).toArray))
    val geoAbsCon = geoAbsRdd.map(x => {
      val geoConTmp = new ArrayBuffer[Double]()
      for (i <- featureImportance.indices)
        if (featureImportance(i) != 0.0) geoConTmp += x._2(i)
      (x._1, Vectors.dense(geoConTmp.toArray))
    })

    sparkSession.createDataFrame(geoAbsCon).toDF(key, feature)
  }

  def reduceDimension(geoCon: DataFrame, n: Int, key: String, feature: String, sparkSession: SparkSession): DataFrame = {

    var reducedRes = pca(geoCon, n, feature, s"pca$feature")
    reducedRes = reducedRes.select(key, s"pca$feature")
    reducedRes = reducedRes.withColumnRenamed(s"pca$feature", feature)
    reducedRes
  }

}
