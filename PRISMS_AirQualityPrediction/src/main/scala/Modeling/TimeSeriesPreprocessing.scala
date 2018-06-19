package Modeling

import WinOps.WindowSession
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer


object TimeSeriesPreprocessing {

  def dataCleaning (observation: DataFrame, columnSet: List[String],
                          config: Map[String, Any]): DataFrame = {

    val locationColumn = columnSet.head
    val timeColumn = columnSet(1)
    val valueColumn = columnSet(2)
    val windowSize = config("window_size").asInstanceOf[Double].toInt
    val windowMethod = config("window_method").asInstanceOf[String]
    val unixTimeColumn = config("unix_time_column").asInstanceOf[String]
    val labelColumn = config("label_column").asInstanceOf[String]

    //    Add unix_time col for getting sliding window
    var newObservation = observation.withColumn(unixTimeColumn, functions.unix_timestamp(observation.col(timeColumn)))  // * nanoSeconds)

    //    Compute moving mean and median in w hr sliding window
    val win = new WindowSession(locationColumn, unixTimeColumn)
    win.createWindowSpecByRange(windowSize * 3600)
    val windowSpec = win.getWindowSpec

    //    Pick median in the missing value location
    newObservation = newObservation.withColumn("mean", functions.mean(newObservation.col(valueColumn)).over(windowSpec))
    newObservation = newObservation.withColumn("median", WinOps.Median(newObservation.col(valueColumn)).over(windowSpec))
    newObservation = newObservation.withColumn(labelColumn, functions.when(newObservation.col(valueColumn).isNotNull,
      newObservation.col(valueColumn)).otherwise(newObservation.col(windowMethod)))

    newObservation
  }

  def timeSeriesConstruction (observation: DataFrame, columnSet: List[String],
                              config: Map[String, Any], sparkSession : SparkSession):

  DataFrame = {

    /*
       Convert column-like air quality data to row-like time series data
       e.g. station, time, val => time1, time2, time3...
    */

    val locationColumn = columnSet.head
    val timeColumn = columnSet(1)
    val valueColumn = columnSet(2)
    val unixTimeColumn = config("unix_time_Column").asInstanceOf[String]
    val labelColumn = config("label_column").asInstanceOf[String]

    val observationRdd = observation.rdd.map(x => (x.getAs[String](locationColumn), x.getAs[Long](unixTimeColumn), x.getAs[Double](labelColumn)))
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

    sparkSession.createDataFrame(timeSeriesRdd).toDF(locationColumn, "time_series")
  }
}
