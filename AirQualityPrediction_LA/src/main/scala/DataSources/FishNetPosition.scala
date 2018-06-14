package DataSources
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
object FishNetPosition {
  def fishNetPosition(sparkSession:SparkSession):DataFrame={
    val schema = "los_angeles"
    val tableName ="los_angeles_fishnet"
    val cols = List("gid","ST_X(geom) AS log", "ST_Y(geom) AS lat")
    val airnowDF = DBConnection.dbReadData(schema, tableName, cols, "", sparkSession)
    airnowDF
  }
}
