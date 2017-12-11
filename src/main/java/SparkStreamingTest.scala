import java.io.{File, FileInputStream}
import java.util.Properties
import scala.collection.JavaConverters._

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentAnnotatedTree
import edu.stanford.nlp.util.CoreMap
import org.apache.spark.sql._
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.yaml.snakeyaml.Yaml
import twitter4j.GeoLocation

object SparkStreamingTest {

  def main(args: Array[String]):Unit = {

    //Get file from resources folder//Get file from resources folder

    val path = getClass.getResource("twitter_auth.yaml")
      .getPath
      .replace("%20", " ") // Escape the path (If applicable)
    val file = new File(path)

    val ios = new FileInputStream(file)
    val yaml = new Yaml()
    val obj = yaml.load(ios).asInstanceOf[java.util.Map[String, Any]]

    val consumerKey:String = obj.get("consumerKey").toString
    val consumerSecret:String = obj.get("consumerSecret").toString
    val accessToken:String = obj.get("accessToken").toString
    val accessTokenSecret:String = obj.get("accessTokenSecret").toString

    print(consumerKey, consumerSecret, accessToken, accessTokenSecret)

    val config = new SparkConf()
      .setAppName("twitter-stream-sentiment")
      .setMaster("local[2]") // Use 2 cores on the local machine
      .set("spark.sql.parquet.mergeSchema", "true")

    // Configure Twitter API
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sc = new SparkContext(config)
    val batchSize:Duration = Seconds(10) // A new RDD will be processed for each "batch"
    sc.setLogLevel("INFO")
    val ssc = new StreamingContext(sc, batchSize)
    val stream = TwitterUtils.createStream(ssc, None)

    // Get the singleton instance of SQLContext
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val languagesToWrite = List("en", "fr", "it")

    stream
      .foreachRDD(rdd => {
      // Convert RDD[String] to DataFrame
      val wordsDataFrame = rdd
        .filter(
          w => languagesToWrite.contains(w.getLang)
        ) // Only pull out english tweets
        .map(w => Record(
          text = w.getText,
          created_data = w.getCreatedAt.getTime,
          id = w.getId,
          latitude = w.getGeoLocation match {
            case myGeo:GeoLocation => myGeo.getLatitude
            case null => 0.0
          },
          longitude = w.getGeoLocation match {
            case myGeo:GeoLocation => myGeo.getLongitude
            case null => 0.0
          },
          favourite_count = w.getFavoriteCount,
          retweet_count = w.getRetweetCount,
          isSensitive = w.isPossiblySensitive,
          lang = w.getLang,
          sentiment = sentiment(w.getText)
        )
      ).toDF("text", "created_date", "id", "latitude", "longitude", "favourite_count", "retweet_count", "isSensitive", "lang", "sentiment")

      wordsDataFrame
        .coalesce(1)
        .write
        .partitionBy("lang", "sentiment")
        .format("parquet")
        .option("compression","snappy")
        .mode(SaveMode.Append)
        .save("/Users/Matthew/twitter/table")
    })

    ssc.start()
    Thread.sleep(3 * 60 * 1000) // Grab 3 minutes of data
    ssc.stop(stopSparkContext = true, stopGracefully = true) // Finish writing out after 3 minutes
  }

  def sentiment(s: String):String = {
    val props: Properties = new Properties()
    props.put("annotators", "tokenize, ssplit, parse, sentiment")

    // create blank annotator
    val document1: Annotation = new Annotation(s)
    val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)
    // run all Annotators
    pipeline.annotate(document1)

    val sentences1: List[CoreMap] = document1.get(classOf[CoreAnnotations.SentencesAnnotation]).asScala.toList

    val processed = sentences1
      .map(sentence => (sentence, sentence.get(classOf[SentimentAnnotatedTree])))
      .map { case (sentence, tree) => (sentence.toString, getSentiment(RNNCoreAnnotations.getPredictedClass(tree))) }

    // Return this
    processed.head._2
  }

  //Helper method
  def getSentiment(sentiment: Int): String = sentiment match {
    case x if x == 0 || x == 1 => "Negative"
    case 2 => "Neutral"
    case x if x == 3 || x == 4 => "Positive"
  }
}

// Helper class to give a record a set schema
case class Record(text: String, created_data: Long, id: Long,
                  latitude: Double, longitude: Double, favourite_count: Int,
                  retweet_count: Int, isSensitive: Boolean, lang: String, sentiment: String)