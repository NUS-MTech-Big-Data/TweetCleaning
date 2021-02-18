import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Main extends App {
  def tweetSchema(): StructType = {
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

  val spark = SparkSession.builder.appName("TweetCleaning").master("local[*]").getOrCreate()
  val df_kafka = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "192.168.1.77:9092")
    .option("subscribe", "twitter.raw")
    .option("startingOffsets", "earliest")
    .load()
  df_kafka.printSchema()

  val df = df_kafka.selectExpr("CAST(value AS STRING)")
  val df_json = df.select(from_json(col("value"), tweetSchema()).alias("parsed"))
  val query = df_json.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .start()
  df_json.printSchema()

  query.awaitTermination()
}