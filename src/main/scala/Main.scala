import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Main extends App {
  val spark = SparkSession.builder.appName("TweetCleaning").master("local[*]").getOrCreate()
  val df_raw = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "192.168.1.77:9092")
    .option("subscribe", "twitter.raw")
    .option("startingOffsets", "earliest")
    .load()
  df_raw.printSchema()

  val df = df_raw.selectExpr("CAST(value AS STRING)") // cast value from bytes to string
  val df_json = df.select(from_json(col("value"), Tweet.schema()).alias("parsed"))
  df_json.printSchema()

  val df_clean = df_json.select(
    col("parsed.payload.Id").cast("string").alias("key"),
    to_json(col("parsed.payload")).alias("value")
  )
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "192.168.1.77:9092")
    .option("topic", "twitter.clean")
    .option("checkpointLocation", "/Users/ry735172/Projects/nus/TweetCleaning")
    .start()
  df_clean.awaitTermination()
}

object Tweet {
  def schema(): StructType = {
    val userSchema = new StructType()
      .add("Id", LongType)
      .add("Name", StringType)
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

