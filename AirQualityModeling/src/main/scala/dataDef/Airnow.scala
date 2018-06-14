package dataDef

import java.sql.{Connection, ResultSet, Timestamp}
import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer


class Airnow (_reportingArea: String,
              _dateObserved: String,
              _aqi: Double) extends java.io.Serializable {

  var reportingArea = _reportingArea
  var dateObserved = new Time(_dateObserved)
  var aqi = _aqi


}
