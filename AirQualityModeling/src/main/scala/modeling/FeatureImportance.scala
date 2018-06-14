package modeling

import java.io.{File, PrintWriter}

import dataDef.ClusterRes
import org.apache.spark.ml
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object FeatureImportance {

  def calFeatureImportance(trainingFeatureVector: ArrayBuffer[(String, Array[Double])],
                           clusterResult: RDD[ClusterRes],
                           spark : SparkSession):
  Array[Double] = {

    val sc = spark.sparkContext
    val trainingFeatureVectorRDD = sc.parallelize(trainingFeatureVector)
    val trainingFeatureVectorMap = trainingFeatureVectorRDD.collectAsMap()

    val featureVector = clusterResult.map(x => (x.cluster, trainingFeatureVectorMap(x.reportingArea)))

    import spark.implicits._

    // Convert RDD to Dataframe
    val geoFeatureVectorDF = featureVector.map(x => (x._1, ml.linalg.Vectors.dense(x._2))).toDF("label", "features")


    // Random forest model set up
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(500)
      .setMaxDepth(30)
      .setFeatureSubsetStrategy("auto")
      .setSeed(5043)


    // Train model
    val model : RandomForestClassificationModel = rf.fit(geoFeatureVectorDF)
//    val predictions = model.transform(geoFeatureVectorDF)

    val featureImportance = model.featureImportances.toDense.toArray

    return featureImportance
  }


  def partImportance(featureTypeName: ArrayBuffer[(String, Int, String)],
                     featureImportance: Array[Double],
                     percentage: Int):
  Array[Double] = {

    val featureImportanceTmp = new ArrayBuffer[((String, Int, String), Double)]()
    var i = 0
    while(i < featureTypeName.size) {
      featureImportanceTmp += ((featureTypeName(i), featureImportance(i)))
      i += 1
    }

    val featureImportanceSorted = featureImportanceTmp.sortBy(_._2).reverse
    val featureImportanceSortedMap = new mutable.HashMap[(String, Int, String), Double]()

    var j = 0
    var sum = 0.0
    while(j < featureImportanceSorted.size) {
      if (j <= percentage * featureImportanceSorted.size / 100) {
        featureImportanceSortedMap(featureImportanceSorted(j)._1) = featureImportanceSorted(j)._2
        sum += featureImportanceSorted(j)._2
      }
      if (j > percentage * featureImportanceSorted.size / 100)
        featureImportanceSortedMap(featureImportanceSorted(j)._1) = 0.0
      j += 1
    }

    val partFeatureImportance = new ArrayBuffer[Double]()
    var k = 0
    var count = 0
    while(k < featureImportanceTmp.size) {
      if (featureImportanceSortedMap(featureImportanceTmp(k)._1) == 0.0) {
        count += 1
        partFeatureImportance += 0.0
      }
      else partFeatureImportance += featureImportanceSortedMap(featureImportanceTmp(k)._1)
//      else partFeatureImportance += 1.0
      k += 1
    }

    print(count + ",")
    print(percentage + "%,")
    print(sum + ",")

    return partFeatureImportance.toArray
  }

}
