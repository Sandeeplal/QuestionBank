package example
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext._
import java.io.File
import scala.tools.nsc.classpath.FileUtils
object QBankSQL {
  val conf = new SparkConf().setMaster("local[4]").setAppName("QBank")
  val sc = new SparkContext(conf)
  val sqlc = new org.apache.spark.sql.hive.HiveContext(sc)
  import sqlc.implicits._
  case class Table(qid: String, i: String, qs: String, qt: String="SA", tags: String="SA", qvc: String, qac: String, aid: String, j: String, as: String, at: String="SA")
 //def main(args: Array[String]) {
    val inputDirectory = "D:\\Hadoop\\Projects\\Project with solution\\Project 3\\Project 3_dataset_answers1.csv"
    val rdd1 = sc.textFile(inputDirectory)
    val rdd2 = rdd1.map(s => s.slice(1, s.length() - 1))
    val rdd3 = rdd2.map(s => s.split("_"))

    val rdd4 = rdd3.map(p => Table(p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11)))
    //rdd4.take(10).foreach(println).toString()

    val df = rdd4.toDF()
    // print(df.show(10))
    //df.createOrReplaceTempView("QBANK")
    df.write.mode("append").format("parquet").saveAsTable("QBANKHive")
    // val df1 = sqlc.sql("SELECT * from QBANK")
    //val Hivedf = sqlc.read.load("C:\\Users\\sandeep\\IdeaProjects\\QuestionBank\\spark-warehouse\\qbankhive")
  // val df1 = sqlc.sql("SELECT * from QBANK")

    //Hivedf.show(20)
    //df.show(20)
  }
//}