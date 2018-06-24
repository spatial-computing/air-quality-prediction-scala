package Utils

import java.util.Properties
import org.apache.spark.sql.{DataFrame, SparkSession}

object DBConnectionPostgres {

  def dbJDBC: String = {
    val hostname = "localhost"
    val port = 11223
    val database = "prisms"
    val url = s"jdbc:postgresql://$hostname:$port/$database"
    url
  }

  def connProperties: Properties = {
    val properties = new Properties()
    properties.put("user", "yijun")
    properties.put("password", "m\\tC7;cc")
    properties.put("Driver", "org.postgresql.Driver")
    properties
  }

  def dbReadData(tableName: String,
                 cols: List[String],
                 conditions: String,
                 sparkSession: SparkSession):
  DataFrame = {

    val colString = cols.mkString(",")
    val query = s"(select $colString from $tableName $conditions) as sub"
    val data = sparkSession.read.jdbc(
      url = this.dbJDBC,
      table = query,
      properties = this.connProperties
    )
    data
  }

  def dbWriteData(df: DataFrame,
                  schema: String,
                  tableName: String):
  Unit = {

    df.write.mode("append").jdbc(
      this.dbJDBC,
      schema + '.' + tableName,
      this.connProperties
    )
  }


  /*temporary*/
  def cleanPurpleairId(sparkSession: SparkSession):List[String]={
    val query = "(select distinct sensor_id from others.purpleair_euclidean_distance ed join others.purpleair_los_angeles_channel_a cb on ed.id = cb.parent_id where ed.eu_distance <= 50) as ed "
    val data = sparkSession.read.jdbc(
      url = this.dbJDBC,
      table = query,
      properties = this.connProperties
    )
    data.collect().map(_.getInt(0).toString).toList
  }

}
