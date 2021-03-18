import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object Main extends App {
  val kafkaHost = "192.168.1.77:9092"
  val sparkWriteCheckPoint = "/Users/ry735172/Projects/nus/TweetCleaning/write_checkpoint"
  val spark = SparkSession.builder.appName("TweetCleaning").master("local[*]").getOrCreate()
  // Read tweets from Kafka twitter.raw topic
  val readStream = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafkaHost)
    .option("subscribe", "twitter.raw")
    .option("startingOffsets", "earliest") // Always read from offset 0, for dev/testing purpose
    .option("failOnDataLoss", false)
    .load()
  readStream.printSchema()

  val df = readStream.selectExpr("CAST(value AS STRING)") // cast value from bytes to string
  val df_json = df.select(from_json(col("value"), Tweet.schema()).alias("parsed"))
  df_json.printSchema()

  val df_text = df_json.withColumn("text", col("parsed.payload.Text"))
  val df_clean = DataPreprocessing.cleanTweet(df_text, "text")
  val df_english = DataPreprocessing.filterNonEnglish(df_clean, inputColumn = "text")

  // Write cleaned tweets to Kafka twitter.clean topic
  val df_result = df_english.select(
    col("parsed.payload.Id").cast("string").alias("key"), // key must be string or bytes
    to_json(struct(
      col("parsed.payload.*"),
      col("text") as "FilteredText"
    )).alias("value")
  )
  val writeStream = df_result
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafkaHost)
    .option("topic", "twitter.clean")
    .option("checkpointLocation", sparkWriteCheckPoint)
    .start()
  writeStream.awaitTermination()
}