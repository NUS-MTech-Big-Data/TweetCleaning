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
    .load()
  readStream.printSchema()

  val df = readStream.selectExpr("CAST(value AS STRING)") // cast value from bytes to string
  val df_json = df.select(from_json(col("value"), Tweet.schema()).alias("parsed"))
  df_json.printSchema()

  val df_text = df_json.withColumn("text",
    regexp_replace(regexp_replace(col("parsed.payload.Text"),
      "^RT ", ""),
      "@\\w+", ""))
  val df_english = DataPreprocessing.filterNonEnglish(df_text, inputColumn = "text")
  val df_tokenized = DataPreprocessing.tokenize(df_english, inputColumn = "text", outputColumn = "words")
  val df_filtered = DataPreprocessing.removeStopWords(df_tokenized, inputColumn = "words", outputColumn = "filtered")


  // Write cleaned tweets to Kafka twitter.clean topic
  val df_clean = df_filtered.select(
    col("parsed.payload.Id").cast("string").alias("key"), // key must be string or bytes
    to_json(struct(
      col("parsed.payload.*"),
      concat_ws(" ", col("filtered")) as "FilteredText"
    )).alias("value")
  )
  val writeStream = df_clean
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafkaHost)
    .option("topic", "twitter.clean")
    .option("checkpointLocation", sparkWriteCheckPoint)
    .start()
  writeStream.awaitTermination()
}