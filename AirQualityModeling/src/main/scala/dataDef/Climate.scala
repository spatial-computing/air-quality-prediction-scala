package airnowdata

import dataDef.Time
import org.apache.spark.rdd.RDD

class Climate (_reportingArea: String,
               _time: String,
               _temp: Double,
               _humidity: Double,
               _windBearing: Double,
               _windSpeed: Double
//               _latittude: Double,
//               _longitude: Double,
//               _timezone: String,
//               _precipIntensity: Double,
//               _precipProbability: Double,
//               _pressure: Double,
//               _icon: String,
//               _dewPoint: Double,
//               _apparentTemp: Double,
//               _cloudCover: Double,
//               _uvIndex: Int,
//               _windBearing: Double,
//               _windSpeed: Double,
//               _visibility: Double,
//               _summary: String,

               )  extends java.io.Serializable {

    var reportingArea = _reportingArea
    var time = new Time(_time)
    var temp = _temp
    var humidity =  _humidity
    var windBearing = _windBearing
    var windSpeed = _windSpeed

//    var latittude = _latittude
//    var longitude =_longitude
//    var timezone = _timezone
//    var precipIntensity = _precipIntensity
//    var precipProbability = _precipProbability
//    var pressure = _pressure
//    var icon =_icon
//    var dewPoint =_dewPoint
//    var apparentTemp = _apparentTemp
//    var cloudCover = _cloudCover
//    var visibility = _visibility
//    var uvIndex = _uvIndex
//    var summary = _summary

}
