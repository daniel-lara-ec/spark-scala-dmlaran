name := "dmlaran-spark-udfs"

version := "0.5"

scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.1.1" % Provided
)