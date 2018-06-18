package Modeling

import WinOps.WindowSession
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer


object TimeSeriesProcessing {

  def timeSeriesPreprocess (df: DataFrame, cols: List[String],
                            config: Map[String, Any])
  : DataFrame = {

    val idCol = cols.head
    val timeCol = cols(1)
    val valueCol = cols(2)
    val winSize = config("Win_Size").asInstanceOf[Double].toInt
    val winMethod = config("Win_Method").asInstanceOf[String]
    val unixTimeCol = config("Unix_Time_Col").asInstanceOf[String]
    val winOutputValueCol = config("Output_Label_Col").asInstanceOf[String]

    //    Add unix_time col
    var df_new = df.withColumn(unixTimeCol, functions.unix_timestamp(df.col(timeCol)))  // * nanoSeconds)

    //    Compute moving mean and median in w hr sliding window
    val win = new WindowSession(idCol, unixTimeCol)
    win.createWindowSpecByRange(winSize * 3600)
    val windowSpec = win.getWindowSpec

    //    Pick median in the missing value location
    df_new = df_new.withColumn("mean", functions.mean(df_new.col(valueCol)).over(windowSpec))
    df_new = df_new.withColumn("median", WinOps.Median(df_new.col(valueCol)).over(windowSpec))
    df_new = df_new.withColumn(winOutputValueCol, functions.when(df_new.col(valueCol).isNotNull, df_new.col(valueCol))
                                                 .otherwise(df_new.col(winMethod)))
    df_new
  }

  def timeSeriesConstruction (df: DataFrame, cols: List[String],
                              sparkSession : SparkSession):

  DataFrame = {

    val idCol = cols.head
    val timeCol = cols(1)
    val valueCol = cols(2)

    val observationsRdd = df.rdd.map(x => (x.getAs[String](idCol), x.getAs[Long](timeCol), x.getAs[Double](valueCol)))
    val observationsMap = observationsRdd.map(x => ((x._1, x._2), x._3.asInstanceOf[Double])).collectAsMap()

    val observationsObj = observationsRdd.map(x => x._1).distinct().collect()
    val n = observationsObj.length

    /*
        Count the number of available stations at each timestamp
        Only choose the timestamps that have all the station observations
     */
    val times = observationsRdd.map(x => (x._2, 1)).reduceByKey(_+_)
    val distinctTime = times.filter(_._2 == n).map(_._1).collect()

    val newObservations = new ArrayBuffer[(String, Array[Double])]()
    for (each <- observationsObj)
      newObservations += ((each, distinctTime.map(x => observationsMap((each, x)))))

    val newObservationsRdd = sparkSession.sparkContext.parallelize(newObservations)
      .map(x => (x._1, Vectors.dense(x._2)))

    sparkSession.createDataFrame(newObservationsRdd).toDF(idCol, valueCol)
  }
}
