package util

import breeze.numerics.abs

object Util {
  def AQIToPM25c(AQIPrediction : Double): Double = {

    val aqi = math.round(AQIPrediction)

    if (aqi < 51){
      return ((15.5 - 0) * (aqi - 0) / (51 - 0) + 0).formatted("%.2f").toDouble
    } else if(aqi < 101){
      return ((40.5 - 15.5) * (aqi - 51) / (101 - 51) + 15.5).formatted("%.2f").toDouble
    }else if(aqi < 151){
      return ((65.5 - 40.5) * (aqi - 101) / (151 - 101) + 40.5).formatted("%.2f").toDouble
    }else if(aqi < 201){
      return ((105.5 - 65.5) * (aqi - 151) / (201 - 151) + 65.5).formatted("%.2f").toDouble
    }else if(aqi < 301){
      return ((250.5 - 150.5) * (aqi - 201) / (301 - 201) + 150.5).formatted("%.2f").toDouble
    }else if(aqi < 501){
      return ((500.5 - 250.5) * (aqi - 301) / (501 - 301) + 250.5).formatted("%.2f").toDouble
    }
    return 500
  }

  def pm25cToAQI(pm25cObs : Float):Int = {
    if (pm25cObs < 15.5){
      return math.round((pm25cObs-0)*(51 - 0)/(15.5.toFloat - 0) + 0)
    }else if(pm25cObs < 40.5){
      return math.round((pm25cObs-15.5.toFloat)*(101-51)/(40.5.toFloat - 15.5.toFloat) + 51)
    }else if(pm25cObs < 65.5){
      return math.round((pm25cObs-40.5.toFloat)*(151-101)/(65.5.toFloat - 40.5.toFloat) + 101)
    }else if(pm25cObs < 150.5){
      return math.round((pm25cObs-65.5.toFloat)*(201-151)/(150.5.toFloat - 65.5.toFloat) + 151)
    }else if(pm25cObs < 250.4){
      return math.round((pm25cObs-150.5.toFloat)*(300-201)/(250.4.toFloat - 150.5.toFloat) + 201)
    }else if(pm25cObs <= 500){
      return math.round((pm25cObs-250.5.toFloat)*(500-301)/(500.4.toFloat - 250.5.toFloat) + 301)
    }
    return 500
  }

  def timeDiff(time1: String, time2: String): Int = {
    val tmp1 = time1.substring(11, time1.length).split(":")
    val tmp2 = time2.substring(9, time2.length).split(":")
    val diff = (tmp1(0).toInt - tmp2(0).toInt) * 3600 + (tmp1(1).toInt - tmp2(1).toInt) * 60
    return abs(diff)
  }
}
