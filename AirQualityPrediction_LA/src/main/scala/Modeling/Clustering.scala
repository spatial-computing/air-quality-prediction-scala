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

  def kmeansClustering(observation : DataFrame, iter: Int, k: Int, colMap: Map[String, String],
                       outputCol: String, sparkSession : SparkSession):
  DataFrame = {

    val timeSeries = timeSeriesConstruction(observation, colMap("key"), colMap("time"), colMap("val"), sparkSession)
    val scaledTimeSeries = standardScaler(timeSeries, colMap("val"), s"scaled(${colMap("val")})")
    //input column aqi output column scaled(aqi)
    k_means(scaledTimeSeries, k, iter, s"scaled(${colMap("val")})", outputCol)
    /*+------------------+--------------------+--------------------+-------+
      |         sensor_id|                 aqi|         scaled(aqi)|cluster|
      +------------------+--------------------+--------------------+-------+
      | W San Gabriel Vly|[78.0,56.0,36.0,7...|[2.64110490386389...|      5|
      |   San Gabriel Mts|[51.0,17.0,35.0,6...|[-0.4110669111072...|      6|
      | SW San Bernardino|[61.0,26.0,44.666...|[0.71936709443763...|      1|
      |W San Fernando Vly|[55.0,36.0,54.0,7...|[0.04110669111072...|      3|
      |E San Fernando Vly|[55.0,36.0,54.0,7...|[0.04110669111072...|      3|
      |     NW Coastal LA|[52.0,36.0,21.25,...|[-0.2980235105527...|      7|
      | Santa Clarita Vly|[43.0,12.0,50.0,6...|[-1.3154141155431...|      2|
      |     SW Coastal LA|[52.0,36.0,21.25,...|[-0.2980235105527...|      7|
      | E San Gabriel V-2|[51.0,17.0,35.0,2...|[-0.4110669111072...|      4|
      |  South Coastal LA|[52.0,36.0,21.25,...|[-0.2980235105527...|      7|
      |   Southeast LA CO|[51.0,43.0,17.0,6...|[-0.4110669111072...|      0|
      +------------------+--------------------+--------------------+-------+*/
  }

  def timeSeriesConstruction (observation: DataFrame,
                              keyCol: String, timeCol: String, valCol: String,
                              sparkSession : SparkSession):

  DataFrame = {

    val obsRdd = observation.rdd.map(x => (x.getAs[String](keyCol), x.getAs[Long](timeCol), x.getAs[Float](valCol)))
    //keyCol:sensorid
    val obsMap = obsRdd.map{case(key,time,value) => ((key, time), value.toString.toDouble)}.collectAsMap()
    //toString toDouble
    val obsObj = obsRdd.map(x => x._1).distinct().collect()
    val n = obsObj.length
    //n here is 11
    val times = obsRdd.map(x => (x._2, 1)).reduceByKey(_+_)
    val distinctTime = times.filter(_._2 == n).map(_._1).collect()
    //at this time(one hour or day or month) every sensor has a value
    val newObs = new ArrayBuffer[(String, Array[Double])]()
    for (each <- obsObj)
      newObs += ((each, distinctTime.map(x => obsMap((each, x)))))

    val newObsRdd = sparkSession.sparkContext.parallelize(newObs)
      .map(x => (x._1, Vectors.dense(x._2)))

    sparkSession.createDataFrame(newObsRdd).toDF(keyCol, valCol)

    //sensor name all time with value so null value in DF
    /*  +------------------+--------------------+
        |         sensor_id|                 aqi|
        +------------------+--------------------+
        | W San Gabriel Vly|[78.0,56.0,36.0,7...|
        |   San Gabriel Mts|[51.0,17.0,35.0,6...|
        | SW San Bernardino|[61.0,26.0,44.666...|
        |W San Fernando Vly|[55.0,36.0,54.0,7...|
        |E San Fernando Vly|[55.0,36.0,54.0,7...|
        |     NW Coastal LA|[52.0,36.0,21.25,...|
        | Santa Clarita Vly|[43.0,12.0,50.0,6...|
        |     SW Coastal LA|[52.0,36.0,21.25,...|
        | E San Gabriel V-2|[51.0,17.0,35.0,2...|
        |  South Coastal LA|[52.0,36.0,21.25,...|
        |   Southeast LA CO|[51.0,43.0,17.0,6...|
        +------------------+--------------------+*/
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