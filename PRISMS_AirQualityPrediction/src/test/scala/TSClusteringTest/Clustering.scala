package TSClusteringTest

import Utils.Consts
import WinOps.WindowSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object Clustering {

//  def main(args: Array[String]): Unit = {
//
//    Logger.getLogger("org").setLevel(Level.OFF)
//    val sparkSession = SparkSession
//      .builder()
//      .appName("ClusterTest")
//      .config("spark.master", "local")
//      .getOrCreate()
//
////    val tableName = Consts.beijing_tablename
////    val cols = Consts.beijing_cols
////    val conditions = "where pm25 is not NULL " +
////      "and date_observed >= '2017-01-01 00:00:00' and date_observed < '2018-01-01 00:00:00'"
//
//    val tableName = Consts.airnow_reporting_area_tablename
//    val cols = Consts.airnow_reporting_area_cols
//    val conditions = "where parameter_name = 'PM2.5' " +
//      " and date_observed >= '2017-01-01 00:00:00' and date_observed < '2018-01-01 00:00:00'"
////      + reporting_area != 'E San Gabriel V-1' and reporting_area != 'Antelope Vly'
//
//    val ts = tsConstruction(tableName, cols, conditions, sparkSession)
//    val n = ts.select(cols.head).distinct().count()
//
//    for (k <- 2 to n.asInstanceOf[Int]) {
//      val res = TimeSeriesClustering.kMeansClustering(ts, "value", "cluster", k, 100, 35)
//        .select(cols.head, "cluster")
////      res.show(20)
//
//      val res1 = res.groupBy("cluster").agg(functions.collect_list(res.col(cols.head)).as("set"))
//      res1.show()
//    }
//  }


}
