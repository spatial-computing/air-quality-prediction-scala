package Utils

import org.apache.spark.sql.{DataFrame, functions}

object TimeUtils {

  def getUnixTimestamp(df: DataFrame,
                       timeCol: String, outputCol: String): DataFrame = {

    df.withColumn(outputCol, functions.unix_timestamp(df.col(timeCol)))  // * nanoSeconds)
  }



}
