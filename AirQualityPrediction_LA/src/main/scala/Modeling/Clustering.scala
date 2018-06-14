package Modeling

import Utils.MLUtils.{standardScaler, k_means}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import scala.collection.mutable.ArrayBuffer

object Clustering {

  def kmeansClustering(observation: DataFrame, iter: Int,
                       keyCol: String, timeCol: String, valCol: String,
                       sparkSession : SparkSession):
  Unit = {

    val numKey = observation.select(keyCol).distinct().count()
    val timeSeries = timeSeriesConstruction(observation, keyCol, timeCol, valCol, sparkSession)
    val scaledTimeSeries = standardScaler(timeSeries, valCol, s"scaled($valCol)")

    for (k <- 2 to numKey.asInstanceOf[Int])
      k_means(scaledTimeSeries, k, iter, s"scaled($valCol)", "cluster")

  }

  def kmeansClustering(observation : DataFrame, iter: Int, k: Int, c: Map[String, String],
                       outputCol: String, sparkSession : SparkSession):
  DataFrame = {

    val timeSeries = timeSeriesConstruction(observation, c("key"), c("time"), c("val"), sparkSession)
    val scaledTimeSeries = standardScaler(timeSeries, c("val"), s"scaled(${c("val")})")
    k_means(scaledTimeSeries, k, iter, s"scaled(${c("val")})", outputCol)
  }

  def timeSeriesConstruction (observation: DataFrame,
                              keyCol: String, timeCol: String, valCol: String,
                              sparkSession : SparkSession):

  DataFrame = {

    val obsRdd = observation.rdd.map(x => (x.getAs[String](keyCol), x.getAs[Long](timeCol), x.getAs[Double](valCol)))
    val obsMap = obsRdd.map(x => ((x._1, x._2), x._3.toString.toDouble)).collectAsMap()

    val obsObj = obsRdd.map(x => x._1).distinct().collect()
    val n = obsObj.length

    val times = obsRdd.map(x => (x._2, 1)).reduceByKey(_+_)
    val distinctTime = times.filter(_._2 == n).map(_._1).collect()

    val newObs = new ArrayBuffer[(String, Array[Double])]()
    for (each <- obsObj)
      newObs += ((each, distinctTime.map(x => obsMap((each, x)))))

    val newObsRdd = sparkSession.sparkContext.parallelize(newObs)
      .map(x => (x._1, Vectors.dense(x._2)))

    sparkSession.createDataFrame(newObsRdd).toDF(keyCol, valCol)
  }
}

/* part for using Time series Method

//    val numKeys = observation.select(keyCol).distinct().count()
//    val dtHead = observation.agg(functions.min(unixtimeCol)).head().getAs[Long](s"min($unixtimeCol)")
//    val dtTail = observation.agg(functions.max(unixtimeCol)).head().getAs[Long](s"max($unixtimeCol)")
//    val dtIndex = DateTimeIndex.uniformFromInterval(dtHead, dtTail, new HourFrequency(1))
//    var dt = observation.groupBy(unixtimeCol).agg(functions.count(keyCol).as(s"count($keyCol)"))
//    dt = dt.filter(dt.col(s"count($keyCol)") === numKeys)
//    val dtArray = dt.rdd.map(x => x.getAs[Long](unixtimeCol)).collect()
//    val dtIndex = DateTimeIndex.irregular(dtArray)
//    val timeSeries = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex, observation, timestampCol, keyCol, valCol)
//    val timeSeriesRDD = timeSeries.map(x => (x._1, Vectors.dense(x._2.toArray)))

*/