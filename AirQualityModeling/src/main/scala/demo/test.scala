package demo
import org.apache.commons.net.ntp.TimeStamp
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.Vectors
import org.joda.time.DateTime

  import org.apache.spark.sql.SparkSession

object test {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF) // Not show information

    val spark = SparkSession.builder
      .appName("AqiPrediction")
      .config("spark.master", "local")
      .getOrCreate()

    import java.text.SimpleDateFormat

    val inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = "2015-01-31 12:34:00-7"
    val in = (inputFormat.parse(date)).getTime
    println(in)
    println(in)
    val a = new DateTime(in)
    println(a)

//
    import spark.implicits._

//
//    val trainingDF = spark.createDataFrame(Seq(
//      (0, 60.5, Vectors.dense(123.446, 0.1, -10.0)),
//      (1, 34.2, Vectors.dense(2.230, 11.0, 10.0)),
//      (2, 24.5, Vectors.dense(35.235, 1.0, 30.0)),
//      (3, 0.0,  Vectors.dense(20.346, 1.01, 20.0)))).toDF("id", "aqi", "features")
//
//    val scaler = new ml.feature.MinMaxScaler()
//          .setInputCol("features")
//          .setOutputCol("scaledFeatures")
//          .setMin(0)
//          .setMax(1)
//
//    val scalerModel = scaler.fit(trainingDF)
//    val scaledFeature = scalerModel.transform(trainingDF)
//
//    val trainingDF1 = scaledFeature.filter(trainingDF("id") < 3)
//    val testingDF1 = scaledFeature.filter(trainingDF("id") === 3)
////    scaledFeature.select("scaledFeatures").rdd.foreach(println)
//    scaledFeature.select("features").rdd.foreach(println)
//
//
//    //    val testingDF = spark.createDataFrame(Seq(
////      (0, Vectors.dense(2.346, 10100, 10.0))
////    )).toDF("id", "features")
//
////
//    val rf = new ml.regression.RandomForestRegressor()//.GBTRegressor()//.RandomForestRegressor()
//      .setLabelCol("aqi")
//      .setFeaturesCol("features")
//
//
//
//    val model = rf.fit(trainingDF1)
//    val prediction = model.transform(testingDF1)
//
//    val result = prediction.select("id", "prediction")
//    result.show()


  }
}
