import org.apache.spark.sql.{Encoders, SparkSession}

object MovesInfoDataSet extends App {
  val spark = SparkSession.builder().appName("movieinfo").master("local[*]").getOrCreate()
  spark.sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", "::")

  case class movie(ID: String, Title: String, Genres: String)

  implicit val moveiEncoder = Encoders.product[movie]

  import spark.implicits._

  val ratingDF = spark.read
    .option("delimiter", ":")
    .format("com.databricks.spark.csv")
    .option("header", "false") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load("data\\ratings.dat")

  val df2 = ratingDF.select("_c0", "_c2", "_c4", "_c6").toDF("UserID", "MoviesID", "Ratings", "TimesStamps")
  val dff2 = df2.select(df2("MoviesID")).rdd.map(x => (x.mkString, 1)).reduceByKey(_ + _).sortBy(_._2, ascending = false)
    .filter(_._2 > 40).toDF("MoviesID", "Count")

  val movies_withCountRatingDF = df2.join(dff2, df2("MoviesID") === dff2("MoviesID"), "inner")
    .select(df2("UserID"), df2("MoviesID"), $"Ratings", $"Count".as("Views"))
  //.orderBy($"Ratings".desc).limit(20).show() // Q2


  val movieDF = spark.read
    .option("delimiter", ":")
    .format("com.databricks.spark.csv")
    .option("header", "false") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load("data\\movies.dat")

  val df = movieDF.select("_c0", "_c2", "_c4").toDF("MoviesID", "Title", "Genres")

  //df.join(df2, "MoviesID").limit(10).select($"MoviesID",$"Title").orderBy($"Title".asc)show() //Q1


  val usersDF = spark.read
    .option("delimiter", ":")
    .format("com.databricks.spark.csv")
    .option("header", "false") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load("data\\users.dat")

  val df3 = usersDF.select("_c0", "_c2", "_c4", "_c6", "_c8").toDF("UserID", "Gender", "Age", "Occupation", "ZipCode")

  val user_withRatingDF = df3.join(movies_withCountRatingDF, "UserID")
    .select($"UserID", $"Ratings", $"MoviesID", $"Age")
    .orderBy($"Ratings".desc).limit(20)
  //.groupBy($"Age").avg("Ratings")

  /*+---+------------+
  |Age|avg(Ratings)|
    +---+------------+
  | 56|         5.0|
  | 35|         5.0|
  | 25|         5.0|
  | 18|         5.0|
  | 45|         5.0|
  | 50|         5.0|
  +---+------------+*/

  //Question 3
  /*user_withRatingDF.filter($"Age"<20).show()
  user_withRatingDF.filter($"Age">40).show()
  user_withRatingDF.filter($"Age">20 && $"Age"<40 ).show()*/

  //movies_withCountRatingDF.orderBy($"Ratings".asc).limit(10).show()//Ques4
}
