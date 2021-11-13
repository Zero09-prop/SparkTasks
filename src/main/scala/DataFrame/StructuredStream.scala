package DataFrame

import util.SparkTransactor

import scala.concurrent.duration.DurationInt

object StructuredStream extends SparkTransactor {
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val stream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", 9092)
      .option("subscribe", "topic")
      .load()

    val source = stream
      .selectExpr("CAST (key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .filter(x => x._2.contains("ERROR"))

    val query = source.writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination(10.seconds.toMillis)

  }
}
