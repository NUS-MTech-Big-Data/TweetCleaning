import kafka.serializer.StringDecoder
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka
import org.apache.spark.streaming.kafka.OffsetRange

// References
// Apache Spark & Kafka, Streaming Partners
// https://medium.com/@gobiviswaml/apache-spark-kafka-streaming-partners-d492bffb8015

// Twitter Sentiment Analysis
// https://github.com/vspiewak/twitter-sentiment-analysis

class SparkObjects() {

  def GetSparkConf(): SparkConf ={
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("TweetCleaning")
    conf
  }

  def GetStreamingContext(): StreamingContext ={
    new StreamingContext(GetSparkConf(), Seconds(10))
  }
}

object Main extends App {
  println("Hello, World from Robin!")


  val ssc = new SparkObjects().GetStreamingContext()
  // Create direct kafka stream with brokers and topics
  val topicsSet = Set("twitter.raw")
  val kafkaParams = Map[String, String]("metadata.broker.list" -> "192.168.1.77:9092","auto.offset.reset" -> "smallest", "enable.auto.commit" -> "false")
  val fromOffsets = Map(
    new TopicPartition("twitter.raw", 0) -> 0
  )

  val kafkaStream = kafka.KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

  /*
  val kafkaStream = kafka.KafkaUtils.createStream(ssc,
    "192.168.1.77:2181","spark-streaming-consumer-group", Map("twitter.raw" -> 1) )
    */
  kafkaStream.print()
  /*
  kafkaStream.foreachRDD{(rdd, time) =>
    rdd.collect().foreach(println)
  }
  */
  kafkaStream.map(tuple => tuple._2).print()
  ssc.start()
  ssc.awaitTermination()
}