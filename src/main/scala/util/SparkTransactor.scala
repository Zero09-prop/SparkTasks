package util

import org.apache.spark.sql.SparkSession
trait SparkTransactor {
  lazy val spark: SparkSession = SparkTransactor.spark
  lazy val config: Map[String, String] = SparkTransactor.config
}
object SparkTransactor {
  val config = Map(
    "url" -> "jdbc:postgresql://127.0.0.1/postgres",
    "user" -> "postgres",
    "password" -> "12345",
    "driver" -> "org.postgresql.Driver"
  )
  lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("SparkPlayground")
    .master("local[*]")
    .getOrCreate()
}
