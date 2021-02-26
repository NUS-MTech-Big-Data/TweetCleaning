import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline

object Main extends App {
  def filterNonEnglish(df: DataFrame): DataFrame = {
    val df_text = df_json.withColumn("text", col("parsed.payload.Text"))
    val df_annotate_result = pipeline.annotate(df_text, "text")
    val df_language = df_annotate_result.withColumn("language", explode(col("language.result")))
    val df_english = df_language.filter(col("language") === "en")
    df_english
  }

  val pipeline = new PretrainedPipeline("detect_language_43", lang = "xx")
  val spark = SparkSession.builder.appName("TweetCleaning").master("local[*]").getOrCreate()

  // Read tweets from Kafka twitter.raw topic
  val df_raw = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "192.168.1.77:9092")
    .option("subscribe", "twitter.raw")
    .option("startingOffsets", "earliest") // Always read from offset 0, for dev/testing purpose
    .load()
  df_raw.printSchema()

  // Parse tweets content from JSON bytes
  val df = df_raw.selectExpr("CAST(value AS STRING)") // cast value from bytes to string
  val df_json = df.select(from_json(col("value"), Tweet.schema()).alias("parsed"))
  df_json.printSchema()

  // Write cleaned tweets to Kafka twitter.clean topic
  val df_clean = filterNonEnglish(df_json).select(
    col("parsed.payload.Id").cast("string").alias("key"), // key must be string or bytes
    to_json(col("parsed.payload")).alias("value")
  )
    .writeStream
    .outputMode("update")
    .format("kafka")
    .option("kafka.bootstrap.servers", "192.168.1.77:9092")
    .option("topic", "twitter.clean")
    .option("checkpointLocation", "/Users/ry735172/Projects/nus/TweetCleaning/write_checkpoint")
    .start()
  df_clean.awaitTermination()
}

object Tweet {
  def schema(): StructType = {
    val userSchema = new StructType()
      .add("Id", LongType)
      .add("Name", StringType)
      .add("ScreenName", StringType) // Twitter User Handle
      .add("Location", StringType)
    val hashTagSchema = new StructType()
      .add("Text", StringType)

    new StructType()
      .add("payload", new StructType()
        .add("CreatedAt", LongType)
        .add("Id", LongType)
        .add("Text", StringType)
        .add("User", userSchema)
        .add("HashtagEntities", ArrayType(hashTagSchema))
      )
  }
}