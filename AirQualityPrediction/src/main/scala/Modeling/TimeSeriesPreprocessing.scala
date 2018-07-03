package Modeling

import WinOps.WindowSession
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer


object TimeSeriesPreprocessing {

  val unixTimeColumnName = "unix_time"
  val aqiLabelColumnName = "label"
  val timeSeriesColumnName = "time_series"

  def dataCleaning(observation: DataFrame, columnSet: List[String],
                   config: Map[String, Any]):
  DataFrame = {

    val location = columnSet.head
    val time = columnSet(1)
    val value = columnSet(2)
    val windowSize = config("window_size").asInstanceOf[Double].toInt
    val windowMethod = config("window_method").asInstanceOf[String]

    //    Add unix_time col for getting sliding window
    var newObservation = observation.withColumn(unixTimeColumnName, functions.unix_timestamp(observation.col(time)))  // * nanoSeconds)

    //    Compute moving mean and median in w hr sliding window
    val win = new WindowSession(location, unixTimeColumnName)
    win.createWindowSpecByRange(windowSize * 3600)
    val windowSpec = win.getWindowSpec

    //    Pick median in the missing value location
    newObservation = newObservation.withColumn("mean", functions.mean(newObservation.col(value)).over(windowSpec))
    newObservation = newObservation.withColumn("median", WinOps.Median(newObservation.col(value)).over(windowSpec))
    newObservation = newObservation.withColumn(aqiLabelColumnName, functions.when(newObservation.col(value).isNotNull,
      newObservation.col(value)).otherwise(newObservation.col(windowMethod)))

    newObservation
  }

  def timeSeriesConstruction(observation: DataFrame, columnSet: List[String],
                             config: Map[String, Any], sparkSession : SparkSession):

  DataFrame = {

    /*
       Convert column-like air quality data to row-like time series data
       e.g. station, time, val => time1, time2, time3...
    */

    val locationColumn = columnSet.head

    val observationRdd = observation.rdd.map(x => (x.getAs[String](locationColumn), x.getAs[Long](unixTimeColumnName), x.getAs[Double](aqiLabelColumnName)))
    val observationMap = observationRdd.map(x => ((x._1, x._2), x._3.asInstanceOf[Double])).collectAsMap()

    val observationLocation = observationRdd.map(x => x._1).distinct().collect()
    val n = observationLocation.length

    /*
        Count the number of available stations at each timestamp
        Only choose the timestamps that have all the station observations
     */
    val times = observationRdd.map(x => (x._2, 1)).reduceByKey(_+_)
    val distinctTime = times.filter(_._2 == n).map(_._1).collect()

    val timeSeries = new ArrayBuffer[(String, Array[Double])]()
    for (each <- observationLocation)
      timeSeries += ((each, distinctTime.map(x => observationMap((each, x)))))

    val timeSeriesRdd = sparkSession.sparkContext.parallelize(timeSeries)
      .map(x => (x._1, Vectors.dense(x._2)))

    sparkSession.createDataFrame(timeSeriesRdd).toDF(locationColumn, timeSeriesColumnName)
  }
}
