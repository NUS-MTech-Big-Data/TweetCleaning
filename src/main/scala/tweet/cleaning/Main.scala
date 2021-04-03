package tweet.cleaning

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, struct, to_json}

object Main extends App {
  val kafkaHost = "localhost:9092"
  val sparkWriteCheckPoint = "write_checkpoint_tweet_cleaning"
  val inputKafkaTopic = "twitter.raw"
  val outputKafkaTopic = "twitter.clean"
  val spark = SparkSession.builder.appName("TweetCleaning").master("local[*]").getOrCreate()
  val hadoopConfig: Configuration = spark.sparkContext.hadoopConfiguration
  hadoopConfig.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
  hadoopConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
  // Read tweets from Kafka twitter.raw topic
  val readStream = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafkaHost)
    .option("subscribe", inputKafkaTopic)
    //    .option("startingOffsets", "earliest") // Always read from offset 0, for dev/testing purpose
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
    .option("topic", outputKafkaTopic)
    .option("checkpointLocation", sparkWriteCheckPoint)
    .start()
  writeStream.awaitTermination()
}
