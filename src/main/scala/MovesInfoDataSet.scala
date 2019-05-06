import org.apache.spark.sql.{Encoders, SparkSession}

object MovesInfoDataSet extends App {
  val spark = SparkSession.builder().appName("movieinfo").master("local[*]").getOrCreate()
  spark.sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", "::")
  case class movie(ID:String, Title:String, Genres:String)
  implicit val moveiEncoder = Encoders.product[movie]


  val ratingDF = spark.read
    .option("delimiter",":")
    .format("com.databricks.spark.csv")
    .option("header", "false") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load("C:\\Users\\Kalyan\\Downloads\\Fragma-Data-master\\Fragma-Data-master\\data\\ratings.dat")
import spark.implicits._
  val df2 = ratingDF.select("_c0","_c2", "_c4","_c6").toDF("UserID","MovieID","Ratings","TimesStamps")
  val dff2 = df2.select(df2("MovieID")).rdd.map(x=>(x.mkString,1)).reduceByKey(_+_).sortBy(_._2, ascending = false)
    .take(10)

  println(dff2.map(_._1).toList)

  val movieDF = spark.read
    .option("delimiter",":")
    .format("com.databricks.spark.csv")
    .option("header", "false") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load("C:\\Users\\Kalyan\\Downloads\\Fragma-Data-master\\Fragma-Data-master\\data\\movies.dat")

  val df = movieDF.select("_c0","_c2", "_c4").toDF("ID","Title","Genres")

  df.select(df("Title")).filter(df("ID")==="2858").show()

  //df.as[movie].show(10)


  val usersDF = spark.read
    .option("delimiter",":")
    .format("com.databricks.spark.csv")
    .option("header", "false") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load("C:\\Users\\Kalyan\\Downloads\\Fragma-Data-master\\Fragma-Data-master\\data\\users.dat")

  //val df3 = usersDF.select("_c0","_c2", "_c4","_c6","_c8").toDF("UserID","Gender","Age","Occupation","ZipCode").show(20)


}
