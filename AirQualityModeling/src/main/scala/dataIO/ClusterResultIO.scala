package dataIO

import java.io.{File, PrintWriter}

import dataDef.{ClimateFeature, ClusterRes}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object ClusterResultIO {

  def writeClusterResult(fileName: String,
                         clusterResult: RDD[ClusterRes]):
  Unit = {

    val comma = ","
    val n = "\n"
    val writer = new PrintWriter(new File(fileName))

    for (eachResult <- clusterResult.collect()) {
      writer.write(eachResult.reportingArea + comma + eachResult.cluster + n)
    }
    writer.close()
  }

  def readClusterResult(fileName: String,
                        spark: SparkSession):
  RDD[ClusterRes] = {

    val ClusterObj = ""
    val text = spark.sparkContext.textFile(fileName)
    val clusterResult = text.map(line => line.split(","))
      .map(x => new ClusterRes(x(0), ClusterObj, x(1).toInt))
    return clusterResult
  }
}
