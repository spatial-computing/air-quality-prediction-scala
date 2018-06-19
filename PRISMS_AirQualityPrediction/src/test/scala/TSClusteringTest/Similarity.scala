package TSClusteringTest

//import TSClusteringTest.Clustering.tsConstruction
import Utils.Consts
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.Correlation

object Similarity {

//  def main(args: Array[String]): Unit = {
//
//    Logger.getLogger("org").setLevel(Level.OFF)
//    val sparkSession = SparkSession
//      .builder()
//      .appName("ClusterTest")
//      .config("spark.master", "local")
//      .getOrCreate()
//
//    val tableName = Consts.beijing_tablename
//    val cols = Consts.beijing_cols
//    val conditions = "where pm25 is not NULL " +
//      "and date_observed >= '2017-01-01 00:00:00' and date_observed < '2018-01-01 00:00:00'"
//
//    //    val tableName = Consts.airnow_reporting_area_tablename
//    //    val cols = Consts.airnow_reporting_area_cols
//    //    val conditions = "where parameter_name = 'PM2.5' " +
//    //      " and date_observed >= '2017-01-01 00:00:00' and date_observed < '2018-01-01 00:00:00'"
//    //      + reporting_area != 'E San Gabriel V-1' and reporting_area != 'Antelope Vly'
//
//    val ts = tsConstruction(tableName, cols, conditions, sparkSession)
////    val n = ts.select(cols.head).distinct().count()
//
//    val corr = Correlation.corr(ts, "value")
//    val Row(coeff1: Matrix) = Correlation.corr(ts, "value").head
//    println("Pearson correlation matrix:\n" + coeff1.toString)


//  }
}
