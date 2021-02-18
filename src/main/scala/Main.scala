import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

// Twitter Sentiment Analysis
// https://github.com/vspiewak/twitter-sentiment-analysis

object Main extends App {
  println("Hello, World from Robin!")
  val spark = SparkSession.builder.appName("TweetCleaning").master("local[*]").getOrCreate()
  val df_raw = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "192.168.1.77:9092")
    .option("subscribe", "twitter.raw")
    .option("startingOffsets", "earliest")
    .load()
  df_raw.printSchema()
  val df = df_raw.selectExpr("CAST(value AS STRING)")
  val query = df.writeStream
    .outputMode("append")
    .format("console")
    .start()

  query.awaitTermination()
}