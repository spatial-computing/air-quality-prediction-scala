package Modeling

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

object TimeSeriesTest {
  def timeSeriesConstruction (observation: DataFrame,purpleairObservation: DataFrame, columnSet: List[String],
                              config: Map[String, Any], sparkSession : SparkSession):

  DataFrame = {

    /*
       Convert column-like air quality data to row-like time series data
       e.g. station, time, val => time1, time2, time3...
    */

    val locationColumn = columnSet.head
    val timeColumn = columnSet(1)
    val valueColumn = columnSet(2)
    val unixTimeColumn = config("unix_time_column").asInstanceOf[String]
    val labelColumn = config("label_column").asInstanceOf[String]

    //val observation = purpleairObservation.union(epaObservation)

    val observationRdd = observation.rdd.map(x => (x.getAs[String](locationColumn), x.getAs[Long](unixTimeColumn), x.getAs[Double](labelColumn)))
    val observationMap = observationRdd.map(x => ((x._1, x._2), x._3.asInstanceOf[Double])).collectAsMap()

    val allTime = purpleairObservation.rdd.map(x => (x.getAs[String](locationColumn), x.getAs[Long](unixTimeColumn), x.getAs[Double](labelColumn))).map(_._2).distinct().collect()

    val locTimePair = observationRdd.map(x=>(x._1,x._2)).groupByKey().map(x=>(x._1,x._2.toArray)).filter(_._2.length >= allTime.length - 31).collect() //time gap missing

    val observationLocation = locTimePair.map(_._1)

    var distinctTime = allTime
    for(each<-locTimePair){

      distinctTime = distinctTime.intersect(each._2)
    }

    //println(distinctTime.length,observationLocation.length)

    val timeSeries = new ArrayBuffer[(String, Array[Double])]()
    for (each <- observationLocation)
      timeSeries += ((each, distinctTime.map(x => observationMap((each, x)))))

    val timeSeriesRdd = sparkSession.sparkContext.parallelize(timeSeries)
      .map(x => (x._1, Vectors.dense(x._2)))

    sparkSession.createDataFrame(timeSeriesRdd).toDF(locationColumn, "time_series")
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
    val unixTimeColumn = config("unix_time_column").asInstanceOf[String]
    val labelColumn = config("label_column").asInstanceOf[String]



    val observationRdd = observation.rdd.map(x => (x.getAs[String](locationColumn), x.getAs[Long](unixTimeColumn), x.getAs[Double](labelColumn)))
    val observationMap = observationRdd.map(x => ((x._1, x._2), x._3.asInstanceOf[Double])).collectAsMap()

    val allTime = observation.rdd.map(x => (x.getAs[String](locationColumn), x.getAs[Long](unixTimeColumn), x.getAs[Double](labelColumn))).map(_._2).distinct().collect()

    val locTimePair = observationRdd.map(x=>(x._1,x._2)).groupByKey().map(x=>(x._1,x._2.toArray)).filter(_._2.length >= allTime.length - 31).collect() //time gap missing

    val observationLocation = locTimePair.map(_._1)

    var distinctTime = allTime
    for(each<-locTimePair){

      distinctTime = distinctTime.intersect(each._2)
    }

    //println(distinctTime.length,observationLocation.length)

    val timeSeries = new ArrayBuffer[(String, Array[Double])]()
    for (each <- observationLocation)
      timeSeries += ((each, distinctTime.map(x => observationMap((each, x)))))

    val timeSeriesRdd = sparkSession.sparkContext.parallelize(timeSeries)
      .map(x => (x._1, Vectors.dense(x._2)))

    sparkSession.createDataFrame(timeSeriesRdd).toDF(locationColumn, "time_series")
  }
}
