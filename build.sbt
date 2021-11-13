name := "SparkTasks"

version := "0.1"

scalaVersion := "2.12.14"
val sparkVersion = "3.1.2"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core",
  "org.apache.spark" %% "spark-sql",
  "org.apache.spark" %% "spark-streaming"
).map(_ % sparkVersion)
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.24"
