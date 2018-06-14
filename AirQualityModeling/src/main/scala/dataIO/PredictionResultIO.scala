package dataIO

import java.io.{File, PrintWriter}

import dataDef.Time
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

object PredictionResultIO {

  def writePredictionResult(time: Long,
                            fishnetGidRDD: RDD[(String, Double, Double)],
                            predictionResult: RDD[(String, Double)]):
  Unit = {

    val fishnetMap = fishnetGidRDD.map(x => (x._1, (x._2, x._3))).collectAsMap()
    var dateTime = new DateTime(time)


    // Write the result into "featureImportance.txt"
    val writer = new PrintWriter(new File("./data/result/" + dateTime.toString() + ".txt"))
    writer.write("gid,prediction,x,y" + "\n")

    for (eachResult <- predictionResult.collect()) {
      val lon = fishnetMap(eachResult._1)._1
      val lat = fishnetMap(eachResult._1)._2
      writer.write(eachResult._1 + "," + eachResult._2 + "," + lon + "," + lat + "\n")
    }
    writer.close()
  }
}
