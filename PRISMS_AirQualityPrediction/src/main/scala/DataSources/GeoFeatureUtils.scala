package DataSources

import org.apache.spark.ml
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

object GeoFeatureUtils {

  def getFeatureNames(geoFeatures: DataFrame, config: Map[String, Any]):
  RDD[(String, String, Int)] = {

    val geoFeaturesCols = config("Geo_Features_Cols").asInstanceOf[List[String]]
    val id = geoFeaturesCols.head
    val geoFeature = geoFeaturesCols(1)
    val featureType = geoFeaturesCols(2)
    val bufferSize = geoFeaturesCols(3)
    val value = geoFeaturesCols(4)

    val distinctFeatures = geoFeatures.drop(id, value)
    val distinctFeaturesRdd = distinctFeatures.distinct()
      .rdd.map(x => (x.getAs[String](geoFeature), x.getAs[String](featureType), x.getAs[Int](bufferSize)))

    distinctFeaturesRdd
  }

  def getImportantFeaturesName(featureName: RDD[(String, String, Int)],
                               featureImportance: Array[Double]): Array[(String, String, Int)] = {

    val featureCollected = featureName.collect()
    val importantFeaturesName = new ArrayBuffer[((String, String, Int))]()

    for (i <- featureImportance.indices)
      if (featureImportance(i) != 0.0)
        importantFeaturesName += ((featureCollected(i)._1, featureCollected(i)._2, featureCollected(i)._3))

    importantFeaturesName.toArray
  }

  def getGeoAbstraction(stations: List[String], geoFeatures: DataFrame, config: Map[String, Any],
                        featureName: RDD[(String, String, Int)], sparkSession: SparkSession):
  DataFrame = {

    val geoFeaturesCols = config("Geo_Features_Cols").asInstanceOf[List[String]]
    val id = geoFeaturesCols.head
    val geoFeature = geoFeaturesCols(1)
    val featureType = geoFeaturesCols(2)
    val bufferSize = geoFeaturesCols(3)
    val value = geoFeaturesCols(4)

    val geoFeaturesMap = geoFeatures.rdd.map(x => ((x.getAs[String](id), x.getAs[String](geoFeature),
      x.getAs[String](featureType), x.getAs[Int](bufferSize)), x.getAs[Double](value))).collectAsMap()

    var geoFeaturesVector = new ArrayBuffer[(String, Array[Double])]
    for (station <- stations) {
      val tmp = featureName.map { x => {
        if (geoFeaturesMap.contains((station, x._1, x._2, x._3)))
          geoFeaturesMap((station, x._1, x._2, x._3))
        else 0.0 //if this sensor contains this kind of feature if yes return val if not return 0.0
        }
      }
      geoFeaturesVector += ((station, tmp.collect()))
    }

    val geoFeaturesRdd = sparkSession.sparkContext.parallelize(geoFeaturesVector)

    sparkSession.createDataFrame(geoFeaturesRdd.map(x => (x._1, Vectors.dense(x._2)))).toDF(id, "geo_features")
  }

  def getGeoContext(geoFeatures: DataFrame, outputId: String, locations: List[String],
                    importantFeaturesName: Array[(String, String, Int)],
                    config: Map[String, Any], sparkSession: SparkSession): DataFrame = {

    val geoFeaturesCols = config("Geo_Features_Cols").asInstanceOf[List[String]]
    val id = geoFeaturesCols.head
    val geoFeature = geoFeaturesCols(1)
    val featureType = geoFeaturesCols(2)
    val bufferSize = geoFeaturesCols(3)
    val value = geoFeaturesCols(4)

    val geoFeaturesMap = geoFeatures.rdd.map(x => ((x.getAs[String](id), x.getAs[String](geoFeature),
      x.getAs[String](featureType), x.getAs[Int](bufferSize)), x.getAs[Double](value)))
      .filter { case ((k, df, ft, bz), v) => importantFeaturesName.contains((df, ft, bz)) }.collectAsMap()

    var featureVector = new ArrayBuffer[(String, Array[Double])]

    for (loc <- locations) {

      val tmp = new ArrayBuffer[Double]()
      for (eachFeature <- importantFeaturesName) {
        if (geoFeaturesMap.contains((loc, eachFeature._1, eachFeature._2, eachFeature._3)))
          tmp += geoFeaturesMap((loc, eachFeature._1, eachFeature._2, eachFeature._3))

        else
          tmp += 0.0 //if this location contains this kind of feature else return 0.0

      }
      featureVector += ((loc, tmp.toArray))
    }

    val geoFeaturesRdd = sparkSession.sparkContext.parallelize(featureVector)
    sparkSession.createDataFrame(geoFeaturesRdd.map(x => (x._1, Vectors.dense(x._2))))
      .toDF(outputId, "geo_features")

  }
}
