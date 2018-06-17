package GeoFeaturesTest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object GenerateFeatures {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    val sparkSession = SparkSession
      .builder()
      .appName("ClusterTest")
      .config("spark.master", "local")
      .getOrCreate()



  }
}
