package example
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext._
import java.io.File
import scala.tools.nsc.classpath.FileUtils
object QBankHiveRead {
  val conf = new SparkConf().setMaster("local[4]").setAppName("QBank")
  val sc = new SparkContext(conf)
  val sqlc = new org.apache.spark.sql.SQLContext(sc)

 def main(args: Array[String]) {

   val Hivedf = sqlc.read.format("parquet").load("C:\\Users\\sandeep\\IdeaProjects\\QuestionBank\\spark-warehouse\\qbankhive")
    // val df1 = sqlc.sql("SELECT * from QBANK")

    Hivedf.show(20)
  }
}