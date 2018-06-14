package DataSources

import org.apache.spark.sql.DataFrame

object CSVIO {

  def CSVWriter(df: DataFrame,
                fileName: String) : Unit = {

    df.coalesce(1).write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save(fileName)
  }
}
