package Demo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import scala.util.parsing.json.JSON
import scala.io.Source


object Main {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkSession = SparkSession
      .builder()
      .appName("air quality prediction")
      .config("spark.master", "local[*]")
      .getOrCreate()

    /*
        Get configuration from json file
     */

//    val configFile = args(0)
    val configFile = "src/data/model/config_yijun.json"
    val lines = Source.fromFile(configFile).mkString.replace("\n", "")
    val config = JSON.parseFull(lines).get.asInstanceOf[Map[String, Any]]
    if (config.isEmpty) {
      println("Fail to parse json file.")
      return
    }

    val testingMethod = config("testing_method").asInstanceOf[String]
    val stageMetrics = ch.cern.sparkmeasure.StageMetrics(sparkSession)

    if (testingMethod == "fishnet")
      stageMetrics.runAndMeasure(FishnetPrediction.prediction(config, sparkSession))

    if (testingMethod == "cross_validation")
       stageMetrics.runAndMeasure(CrossValidation.prediction(config, sparkSession))

//    if (testingMethod == "validation")
//      stageMetrics.runAndMeasure(Validation.prediction(config, sparkSession))

    if (testingMethod == "idw")
      stageMetrics.runAndMeasure(IDWTesting.prediction(config, sparkSession))

    scala.io.StdIn.readLine()

  }
}
