name := "AirQualityPrediction_LA"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.1.0"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

// Spark Essentials
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion
)

// External Packages
libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "9.4.1211.jre7",
  "com.databricks" % "spark-csv_2.11" % "1.5.0",
  "ch.cern.sparkmeasure" %% "spark-measure" % "0.11",
  "com.github.nscala-time" %% "nscala-time" % "2.18.0"
)