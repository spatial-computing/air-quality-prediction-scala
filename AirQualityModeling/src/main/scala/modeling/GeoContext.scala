package modeling

import org.apache.spark.ml
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * Created by yijun on 6/14/17.
  */

object GeoContext {

  def getGeoContext(geoFeatureVector : ArrayBuffer[(String, Array[Double])],
                    featureImportanceArray : Array[Double],
                    spark: SparkSession):
  RDD[(String, Array[Double])] = {

//    ///////////////////////////////////////////////////////////////////////////////////////////////////////////
//    // Scaling Data
//    import spark.implicits._
//
//    val scaler = new ml.feature.MinMaxScaler()
//      .setInputCol("features")
//      .setOutputCol("scaledFeatures")
//      .setMin(0)
//      .setMax(1)
//
//    val featureDF = featureVec.map(x => (x._1, ml.linalg.Vectors.dense(x._2))).toDF("area", "features")
//    val scalerModel = scaler.fit(featureDF)
//    val scaledFeature = scalerModel.transform(featureDF)
//
//    val scaledFeatureRDD = scaledFeature.rdd.map(x => (x.getString(0), x.get(2).toString))
//      .map(x => (x._1, x._2.substring(1, x._2.length - 1).split(",").map(_.toDouble)))

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Compute geo-context column by column
    val geoContextVector : ArrayBuffer[(String, Array[Double])] = new ArrayBuffer[(String, Array[Double])]()

    for(eachFeature <- geoFeatureVector) {
      val tmpArrayBuffer = new ArrayBuffer[Double]()
      var i = 0
      while(i < featureImportanceArray.length) {
        tmpArrayBuffer += eachFeature._2(i) * featureImportanceArray(i)
        i += 1
      }
      geoContextVector += ((eachFeature._1, tmpArrayBuffer.toArray))
    }

    val geoContextRDD = spark.sparkContext.parallelize(geoContextVector)
    return geoContextRDD
  }

}
