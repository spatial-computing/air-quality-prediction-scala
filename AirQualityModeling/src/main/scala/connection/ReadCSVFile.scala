package connection

import scala.collection.mutable.ArrayBuffer
import scala.io.Source


object ReadCSVFile {


  def readFile(fileName: String) : ArrayBuffer[(Int, String, Double, Double, Double, Double)] = {

    val lines = Source.fromFile(fileName).getLines()
    val trajectoryData = new ArrayBuffer[(Int, String, Double, Double, Double, Double)]()

    for (line <- lines) {
      val tmp = line.split(",")
      if (tmp(1).length > 10 && tmp(1).substring(0, 8) == "10/30/17") {
        val gid = tmp(0).toInt
        val gpsTime = tmp(7)
        val lat = tmp(8).toDouble
        val lon = tmp(9).toDouble
        val pm25m = tmp(11).toDouble
        val pm25c = tmp(16).toDouble
        if (gpsTime != "NA" && lat != 0 && lon != 0) {
          trajectoryData.+=((gid, gpsTime, lat, lon, pm25m, pm25c))
        }
      }
    }
    return trajectoryData
  }
}
