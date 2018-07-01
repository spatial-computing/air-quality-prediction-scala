package Modeling

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession, functions}

import scala.collection.Map

object SQLQuery {
  def SQLQuery(validationData:DataFrame ,channelPair:DataFrame ,queryType:String , config: Map[String, Any],sparkSession: SparkSession):DataFrame={

    val schema = new StructType()
    var result = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row],schema)

    val preResult = validationData.join(channelPair,channelPair("sensor_id")===validationData("id"))

    preResult.createOrReplaceTempView("view")

    if(queryType=="max"){
      result = sparkSession.sql("select v.id, v.timestamp, v.aqi from (select pid, max(aqi) as aqi, timestamp from view group by pid, timestamp) as g inner join view as v on g.pid = v.pid and g.aqi = v.aqi and g.timestamp = v.timestamp")

    }
    if(queryType=="min"){

      result = sparkSession.sql("select v.id, v.timestamp, v.aqi from (select pid, min(aqi) as aqi, timestamp from view group by pid, timestamp) as g inner join view as v on g.pid = v.pid and g.aqi = v.aqi and g.timestamp = v.timestamp")
    }
    if(queryType=="avg"){
      /*
          take pid as id
       */
      result = sparkSession.sql("select pid as id, timestamp, avg(aqi) as aqi from view group by pid, timestamp")
    }
    result
  }
}

