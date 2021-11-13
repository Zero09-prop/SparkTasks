package RDD

import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.SparkTransactor

object RddStreaming extends SparkTransactor {
  def main(args: Array[String]): Unit = {
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(1))

    sc.setLogLevel("ERROR")

    ssc.socketTextStream("localhost", 8080).map(s => s.length).print()

    ssc.start()

    ssc.awaitTerminationOrTimeout(10 * 1000)
  }
}
