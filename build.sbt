name := "TweetCleaning"

version := "0.1"

scalaVersion := "2.11.12"


libraryDependencies ++= Seq(
  "com.johnsnowlabs.nlp" %% "spark-nlp" % "2.7.4",
  "org.apache.spark" %% "spark-mllib" % "2.4.7",
  "org.apache.spark" %% "spark-core" % "2.4.7",
  "org.apache.spark" %% "spark-sql" % "2.4.7",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.7"
)

