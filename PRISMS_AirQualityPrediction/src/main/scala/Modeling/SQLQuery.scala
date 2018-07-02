package Modeling

import Utils.DBConnectionPostgres
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession, functions}

import scala.collection.Map

object SQLQuery {
  def SQLQuery(validationData:DataFrame ,queryType:String , config: Map[String, Any],sparkSession: SparkSession):DataFrame={

    val schema = new StructType()
    var result = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row],schema)

    validationData.createOrReplaceTempView("view")

    if(queryType=="max"){
      result = sparkSession.sql("select id, timestamp, max(aqi) as aqi from view group by id, timestamp")
    }
    if(queryType=="min"){

      result = sparkSession.sql("select id, timestamp, min(aqi) as aqi from view group by id, timestamp")
    }
    if(queryType=="avg"){

      result = sparkSession.sql("select id, timestamp, avg(aqi) as aqi from view group by id, timestamp")
    }
    if(queryType=="a"){

      result = sparkSession.sql("select id, timestamp, aqi from view where channel =='a'")
    }
    if(queryType=="b"){

      result = sparkSession.sql("select id, timestamp, aqi from view where channel =='b'")
    }

    result
  }

  def validationDataAndId(validationTableName:String,validationColumnSet:List[String] ,filterState:Boolean, filterOption:String,config:Map[String,Any],sparkSession: SparkSession):(DataFrame,List[String],String,String)={

    var resultTable = ""
    var errTable = ""
    var idSensorIdConvert = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row],new StructType())
    var validationData = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row],new StructType())
    var validationId = List[String]()


    if(!filterState) {

      resultTable = config(s"time_to_time_unfilter_$filterOption").asInstanceOf[List[String]](0)
      errTable = config(s"time_to_time_unfilter_$filterOption").asInstanceOf[List[String]](1)

      idSensorIdConvert = DBConnectionPostgres.dbReadData(config("unfilter_channel").asInstanceOf[String],List("id","sensor_id","channel"),"", sparkSession)

      val validation = DBConnectionPostgres.dbReadData(validationTableName, validationColumnSet, "", sparkSession)
        .join(idSensorIdConvert,"sensor_id")

      validationData = SQLQuery(validation,filterOption,config,sparkSession).cache()

      validationId = validationData.rdd.map(x => x.getAs[Int]("id").toString).distinct().collect().toList

    }

    if(filterState) {

      resultTable = config(s"filtered_channel_$filterOption").asInstanceOf[List[String]](0)
      errTable = config(s"filtered_channel_$filterOption").asInstanceOf[List[String]](1)

      idSensorIdConvert = DBConnectionPostgres.dbReadData(config("unfilter_channel").asInstanceOf[String],List("id","sensor_id","channel"),"", sparkSession)

      val euDist = DBConnectionPostgres.dbReadData("(select * from others.purpleair_euclidean_distance where eu_distance <= 50) as ed",sparkSession)

      validationData = DBConnectionPostgres.dbReadData(validationTableName, validationColumnSet, "", sparkSession)
        .join(idSensorIdConvert ,"sensor_id").filter(s"channel!='$filterOption'").join(euDist ,"id").select("id","timestamp","aqi").cache()

      validationId = validationData.rdd.map(x => x.getAs[Int]("id").toString).distinct().collect().toList
      println(validationId.length)
    }
    (validationData,validationId,resultTable,errTable)
  }
}

