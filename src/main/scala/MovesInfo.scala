import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object MovesInfo extends App {
  import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext
  case class movie(ID:String, TItle:String, Geners:String)

  val sparkConf = new SparkConf().setAppName("FileReader").setMaster("local[*]")
  val sparkContext = new SparkContext(sparkConf)
  // Set custom delimiter for text input format
  sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", "::")
  val sentences : RDD[String] = sparkContext.textFile("C:\\Users\\Kalyan\\Downloads\\Fragma-Data-master\\Fragma-Data-master\\data\\movies.dat")

  val sqlContext :SQLContext = new org.apache.spark.sql.SQLContext(sparkContext)

  val df = sqlContext.createDataFrame(sentences, movie.getClass)
  df.show(10)
}
