package demo

import connection.ConnectDB
import dataDef.{ClusterRes, Time}
import databaseIO.AirnowIO
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.clustering.{BisectingKMeans, KMeans, KMeansModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

object AirQualityDBMain {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    val spark = SparkSession.builder
      .appName("AqiPrediction")
      .config("spark.master", "local")
      .getOrCreate()

    // Connect to database
    val connection = ConnectDB.connectdb(spark)

    // Get sensor data (reporting area, time, aqi)
    val timeResolution = "daily"
    val airnowRDD = AirnowIO.readAirowDataFromDB(timeResolution, spark, connection)
    val station = "sensor_id"
//    val station = "reporting_area"

    val colNames = Seq(station, "time", "aqi")
    val dataRDD = airnowRDD.map(x => (x.reportingArea, x.dateObserved.intTime, x.aqi))
//    RDD -> DF
    import spark.implicits._
    val df = dataRDD.toDF(colNames: _*)

    val key = "time"

    // add average col as y
    val datawithY = computeY(df, key)
//    datawithY.show()
    // compute RMSE
    val RMSE = RMSE_Score(datawithY)
    // compute MAE
    val MAE = MAE_Score(datawithY)

    // export
    toCSV("LA", RMSE, "RMSE")
    toCSV("LA", MAE, "MAE")


    // correlation matrix
//    val rows: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = df.rdd
//    rows.take(5).foreach(print)
//    val features = rows.map(row => Array((row(0), row(1))))
//    val label = rows.map(row => Array(row(2)))
      // maybe only put numerical features
//    val correlation: Double = Statistics.corr(features, label, "pearson")
  }

  def toCSV(station: String, df: DataFrame, Type: String) : Unit = {
    val filename = station + "-" + Type + ".csv"
    df.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save(filename)
  }

  def computeY(df: DataFrame, key: String): DataFrame = {
    val sum_aqi = df.groupBy(key)
      .agg(sum("aqi").as("sum_aqi"), functions.count(key).as("count"))

    val joined = df.join(sum_aqi, key)
    val joinedDF = joined
      .withColumn("z", (col("sum_aqi") - col("aqi")) / (col("count") - 1))
      .drop("sum_aqi")
    joinedDF
}
  def MAE_Score(df: DataFrame): DataFrame = {
    val station = "sensor_id"
//    val station = "reporting_area"
    val mae = df.groupBy(station)
        .agg((sum(functions.abs(col("z") - col("aqi"))) / functions.count(station)).as("MAE"))
    mae.show()
    mae
  }

  def RMSE_Score(df: DataFrame): DataFrame = {
    val station = "sensor_id"
//    val station = "reporting_area"
    val rmse = df.groupBy(station)
        .agg(sqrt(sum(pow(col("z") - col("aqi"), 2)) / functions.count(station)).as("RMSE"))
    rmse.show()
    rmse

  }




}

