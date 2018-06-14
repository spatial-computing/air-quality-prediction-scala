package dataIO

import java.io.{File, PrintWriter}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object FeatureImportanceIO {


  def writeFeatureImportance(featureImportance: Array[Double],
                             featureTypeName: ArrayBuffer[(String, Int, String)]):
    Unit = {

      // Print out feature importance by order
      var i = 0
      val featureImportanceNotZero = new ArrayBuffer[((String, Int, String), Double)]()
      while(i < featureTypeName.size) {
        if (featureImportance(i) >= 0.0)
          featureImportanceNotZero += ((featureTypeName(i), featureImportance(i)))
        i += 1
      }

      val sortedfeatureImportanceNotZero = featureImportanceNotZero.sortBy(_._2).reverse

      // Write the result into "featureImportance.txt"
      val writer = new PrintWriter(new File("./data/featureImportance.txt"))
      writer.write("feature,buffersize,type,importance" + "\n")

      for (eachResult <- sortedfeatureImportanceNotZero) {
        writer.write(eachResult._1._1 + "," + eachResult._1._2 + "," + eachResult._1._3 + "," + eachResult._2 + "\n")
      }
      writer.close()
  }
}
