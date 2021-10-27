name := "ga4-spark-datasource"
version := "0.1.0"
scalaVersion := "2.12.6"
val sparkVersion = "3.1.2"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "com.google.analytics" % "google-analytics-data" % "0.10.1",
  "org.scalatest" %% "scalatest-flatspec" % "3.2.10" % Test
)
Test / parallelExecution := false
