import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode, regexp_replace}

object DataPreprocessing {
  /**
   * Separate a sentence into array of words
   * any character other than words will be ignored
   */
  def tokenize(df_sentence: DataFrame, inputColumn: String, outputColumn: String): DataFrame = {
    val regexTokenizer = new RegexTokenizer()
      .setInputCol(inputColumn)
      .setOutputCol(outputColumn)
      .setPattern("\\W")
    regexTokenizer.transform(df_sentence)
  }

  /**
   *
   * @param df_tokenized Tokenized dataframe
   * @return Dataframe after removing all the stop words such as i , am , a the
   */
  def removeStopWords (df_tokenized : DataFrame, inputColumn: String, outputColumn: String): DataFrame = {
    val stopWordRemover = new StopWordsRemover()
      .setInputCol(inputColumn)
      .setOutputCol(outputColumn)
    stopWordRemover.transform(df_tokenized)
  }

  val detectLanguagePipeline = new PretrainedPipeline("detect_language_43", lang = "xx")
  /**
   * Filter out non English texts
   * @param df DataFrame contain texts
   * @return A DataFrame with only English texts
   */
  def filterNonEnglish(df: DataFrame, inputColumn: String): DataFrame = {
    val df_annotate_result = detectLanguagePipeline.annotate(df, inputColumn)
    val df_language = df_annotate_result.withColumn("language", explode(col("language.result")))
    val df_english = df_language.filter(col("language") === "en")
    df_english
  }


  /**
   * Format extracted tweets by removing retweets, usernames, urls, unnecessary characters
   */
  def cleanTweet (extractedTweets : DataFrame, inputColumn : String) : DataFrame = {
    val singleLineDataframe =  extractedTweets.withColumn(inputColumn, regexp_replace(col(inputColumn), "[\\r\\n\\n]", "."))
    val nonUrlTweetDataframe  = singleLineDataframe.withColumn(inputColumn, regexp_replace(col(inputColumn), "http\\S+", ""))
    val nonHashTagsTweetDataframe = nonUrlTweetDataframe.withColumn(inputColumn, regexp_replace(col(inputColumn), "#", ""))
    val nonUserNameTweets = nonHashTagsTweetDataframe.withColumn(inputColumn, regexp_replace(col(inputColumn), "@\\w+", ""))
    val noRTDataFrame = nonUserNameTweets.withColumn(inputColumn, regexp_replace(col(inputColumn), "RT", ""))
    val noUrlTweetDataframe  = noRTDataFrame.withColumn(inputColumn, regexp_replace(col(inputColumn), "www\\S+", ""))
    val removeUnnecessaryCharacter  = noUrlTweetDataframe.withColumn(inputColumn, regexp_replace(col(inputColumn), ":", ""))
    removeUnnecessaryCharacter
  }
}
