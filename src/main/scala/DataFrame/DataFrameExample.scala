package DataFrame

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import util.SparkTransactor
case class Student(id_student: BigInt, name: String, age: Option[Int])
case class Exam(id_exam: Int, subj_name: String,id_student:BigInt, mark: Int)
object DataFrameExample extends SparkTransactor {
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    val students = spark.read
      .format("jdbc")
      .options(config)
      .option("dbtable", "students")
      .load()
      .as[Student]

    val exams = spark.read
      .format("jdbc")
      .options(config)
      .option("dbtable", "exam")
      .load()
      .as[Exam]

    val joinDf = students
      .join(exams, "id_student")
      .select("id_student", "name", "subj_name", "mark")

    joinDf.createTempView("JoinDf")

    spark.sql("SELECT * FROM JoinDf ORDER BY id_student").show()

    val windowSpec = Window.partitionBy("subj_name").orderBy("mark")

    println("Students exam results and average mark for each subject")

    joinDf
      .withColumn(
        "avgMark",
        avg("mark")
          .over(windowSpec)
      )
      .orderBy("id_student")
      .show()

    println("Top best students in every subject")
    joinDf
      .withColumn(
        "rankMark",
        rank()
          .over(windowSpec)
      )
      .show()

    println("Offset tables")
    joinDf
      .withColumn(
        "lag",
        lag("mark", 1).over(windowSpec)
      )
      .show()

    joinDf
      .withColumn(
        "lead",
        lead("mark", 1).over(windowSpec)
      )
      .show()
  }
}
