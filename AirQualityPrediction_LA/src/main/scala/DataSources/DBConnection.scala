package DataSources

import java.util.Properties
import org.apache.spark.sql.{DataFrame, SparkSession}

object DBConnection {

  def dbJDBC: String = {
    val hostname = "localhost"
    //run on local = 11223, run on server = 5432
    val port = 5432
    val database = "prisms"
    val url = s"jdbc:postgresql://$hostname:$port/$database"
    url
  }

  def connProperties: Properties = {
    val properties = new Properties()
    properties.put("user", "jjow")
    properties.put("password", "m\\tC7;cc")
    properties.put("Driver", "org.postgresql.Driver")
    properties
  }

  def dbReadData(schema: String, tableName: String, cols: List[String],
                 conditions: String, sparkSession: SparkSession): DataFrame = {

    val colString = cols.mkString(",")
    val query = s"(select $colString from $schema.$tableName $conditions) as sub"
    val data = sparkSession.read.jdbc(
      url = this.dbJDBC,
      table = query,
      properties = this.connProperties
    )
    data
  }

  def dbReadDataFishNet(tableName: String, cols: List[String],
                 conditions: String, sparkSession: SparkSession): DataFrame = {

    val colString = cols.mkString(",")
    val query = s"(select $colString from $tableName $conditions) as sub"
    val data = sparkSession.read.jdbc(
      url = this.dbJDBC,
      table = query,
      properties = this.connProperties
    )
    data
  }


  def getMaxTimestamp(sparkSession: SparkSession):String={
    val query = s"(SELECT max(date_observed) FROM airnow_reporting_area) as maxtimstamp"
    val data = sparkSession.read.jdbc(
      url = this.dbJDBC,
      table = query,
      properties = this.connProperties
    )
    val dataCollect = data.collect()(0)
    val maxTime = dataCollect.getTimestamp(0).toString.split("\\.")(0)+"-07"
    maxTime
  }

}
