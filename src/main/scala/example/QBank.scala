package example
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext._
import java.io.File
import scala.tools.nsc.classpath.FileUtils
object QBank {
  val conf = new SparkConf().setMaster("local[4]").setAppName("QBank")
  val sc = new SparkContext(conf)
  val sqlc = new org.apache.spark.sql.SQLContext(sc)


//  def main(args: Array[String]) {
    val inputDirectory="D:\\Hadoop\\Projects\\Project with solution\\Project 3\\Project 3_dataset_answers1.csv"
    val rdd1 = sc.textFile(inputDirectory)
    val rdd2 = rdd1.map(s => s.slice(1, s.length() - 1))
    val rdd3 = rdd2.map(s => s.split("_"))

    val rdd4 = rdd3.flatMap(s => s(5).split(","))
    val rdd5 = rdd4.map(s => (s, 1))
    val rdd6 = rdd5.reduceByKey((a, b) => a + b)
    val rdd7 = rdd6.map(i => i.swap)
    val rdd8 = rdd7.sortByKey(false)
    val rdd9 = rdd8.map(i => i.swap)
//    rdd9.take(10).foreach(println).toString()
    val outputDirectory="D:\\Hadoop\\Projects\\Project with solution\\Project 3\\Project 3_answers"
    val outputDir = new File(outputDirectory)
    if (outputDir.exists())
    {
      import org.apache.commons.io.FileUtils
      FileUtils.deleteDirectory(outputDir)

      }

    rdd9.saveAsTextFile(outputDirectory)
  }
//}
