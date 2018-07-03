package Utils

import java.io.{File, PrintWriter}
import java.sql.Timestamp
import java.text.SimpleDateFormat

import Modeling.Prediction
import org.apache.spark.sql.DataFrame

import scala.collection.Map

object OutputUtils {

  def outputPredictionResult(predictionResult: DataFrame, time: Timestamp,
                             config: Map[String, Any]):
  Unit = {

    if (config("write_to_postgres") == true) {
      val tableName = config("prediction_result_table_name").asInstanceOf[String]
      DBConnectionPostgres.dbWriteData(predictionResult, tableName)
    }

    if (config("write_to_mongodb") == true) {
      DBConnectionMongoDB.dbWriteData(predictionResult, time)
    }

    if (config("write_to_csv") == true) {
      val tmpDir = s"src/data/result/$time"
      predictionResult.coalesce(1).write.format("com.databricks.spark.csv").option("header", true).save(tmpDir)
    }
  }

  /*
      This function is only for txt file
   */
  def checkExistingTimes(times: Array[Timestamp],
                         config: Map[String, Any]):
  Array[Timestamp] = {

    val fm = new SimpleDateFormat("yyyy-MM-dd HH:00:00.0")

    /*
        check if time exists
      */
    val path = config("output_path").asInstanceOf[String]
    val dir = new File(path)
    val files = dir.listFiles.filter(_.isFile).map(x => x.toString)
    val existedFiles = files.map(x => x.split("/")(x.split("/").length - 1))
    val existedTimes = existedFiles.map(x => new Timestamp(fm.parse(x.slice(0, x.length - 4)).getTime))

    times.filter(x => !existedTimes.contains(x))
  }

  /*
      This function is only for txt file
   */
  def writePredictionResult(result: DataFrame,
                            time: Timestamp, config: Map[String, Any]):
  Unit = {

    val path = config("output_path")
    val fileName = path + s"$time.txt"

    val resultCollect = result.rdd.map(x => (x.getAs[String]("gid"), x.getAs[Double](Prediction.predictionColumnName),
      x.getAs[Double]("longitude"), x.getAs[Double]("latitude"))).collect()

    val writer = new PrintWriter(new File(fileName))
    writer.write("gid,prediction,longitude,latitude\n")

    for(each <- resultCollect) {
      writer.write(each._1 + "," + each._2 + "," + each._3 + "," + each._4 + "\n")
    }
    writer.close()
  }

}
