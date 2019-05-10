import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object ProductRevenue extends App {
  val spark = SparkUtilities.sparkSession("")

  val productDF = spark.read.option("header", "true").csv("data/WA_Sales_Products_2012-14.csv")

  val revenueDF = productDF.select("Product type", "Product", "Revenue")

  revenueDF.show(5)

  import spark.implicits._

  val window = Window.partitionBy($"Product type").orderBy($"Revenue".desc)

  val revenueDf = revenueDF.withColumn("rank", dense_rank.over(window)).filter($"rank"===2).show// 2nd highest revenue all over

  val reveDiff = max('revenue).over(window) - 'revenue

  revenueDF.select('*, reveDiff.as("RevenueDiff")).show(10)

  val diff = lead('revenue, 1).over(window) // diff b/w columns value in same row

  println(productDF.rdd.getNumPartitions)



}
