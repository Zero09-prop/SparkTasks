package RDD

import util.SparkTransactor

object RddExample extends SparkTransactor {
  def main(args: Array[String]): Unit = {
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val rdd = sc.parallelize(1 to 100)
    val a = rdd.filter(_ % 2 == 0)
    val b = rdd.filter(_ % 2 != 0)
    val union = a.union(b)
    println("With ordered")
    union
      .takeOrdered(10)
      .foreach(println)
    println("Without ordered")
    union
      .takeSample(withReplacement = false, 10)
      .foreach(println)
    val rddFile = sc.textFile("./text.txt")
    val c = rddFile
      .flatMap(_.split("\\s"))
      .map(word => (word, word.length))
      .randomSplit(Array(0.3, 0.7))
    println("Left outer join on text")
    c(0).leftOuterJoin(c(1)).distinct().foreach(println)

  }

}
