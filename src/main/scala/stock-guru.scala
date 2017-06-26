import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._

import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark

import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.io.Source


object SentimentAnalysisUtils {

  val nlpProps = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment")
    props
  }

  def detectSentiment(message: String): Double = {

    val pipeline = new StanfordCoreNLP(nlpProps)

    val annotation = pipeline.process(message)
    var sentiments: ListBuffer[Double] = ListBuffer()
    var sizes: ListBuffer[Int] = ListBuffer()

    var longest = 0
    var mainSentiment = 0

    for (sentence <- annotation.get(classOf[CoreAnnotations.SentencesAnnotation])) {
      val tree = sentence.get(classOf[SentimentCoreAnnotations.AnnotatedTree])
      val sentiment = RNNCoreAnnotations.getPredictedClass(tree)
      val partText = sentence.toString

      if (partText.length() > longest) {
        mainSentiment = sentiment
        longest = partText.length()
      }

      sentiments += sentiment.toDouble
      sizes += partText.length

      // println("debug: " + sentiment)
      // println("size: " + partText.length)

    }

    val averageSentiment:Double = {
      if(sentiments.size > 0) sentiments.sum / sentiments.size
      else -1
    }

    val weightedSentiments = (sentiments, sizes).zipped.map((sentiment, size) => sentiment * size)
    var weightedSentiment = weightedSentiments.sum / (sizes.fold(0)(_ + _))

    if(sentiments.size == 0) {
      mainSentiment = -1
      weightedSentiment = -1
    }

    // println("debug: main: " + mainSentiment)
    // println("debug: avg: " + averageSentiment)
    // println("debug: weighted: " + weightedSentiment)

    return weightedSentiment
  }
}

object TwitterAnalysisUtils {

  def companiesMentioned(companies : List[List[String]], text: String) : List[String] = {
    val compMent = ListBuffer[String]()
    val textWords = text.split(" ")
    for(comp <- companies){
      val found = false
      for(tag <- comp){
        if(textWords.contains(tag) && found == false){
          compMent += comp(0)
        }
      }
    }
    return compMent.toList
  }

  def checkCompanies(companies : List[List[String]], text: String) : Boolean = {
    val textWords = text.split(" ")
    for(comp <- companies){
      for(tag <- comp){
        if(textWords.contains(tag)){
          return true
        }
      }
    }
    return false
  }

  def getTags(): List[List[String]] = {
      // each row is an array of strings (the columns in the csv file)
      val rows = ListBuffer[List[String]]()
      // (1) read the csv data
      using(Source.fromFile("companies&tags.csv")) { source =>
          for (line <- source.getLines) {
              rows += line.split(",").toList
          }
      }
      // (2) print the results
      for (row <- rows) {
          for (tag <- row){
            print(tag + "\t")
          }
          println()
      }
      def using[A <: { def close(): Unit }, B](resource: A)(f: A => B): B =
          try {
              f(resource)
          } finally {
              resource.close()
          }
      return rows.toList
    }

}


object TwitterStream {
  def main(args: Array[String]){
    // Collect 10 tweets from the current twitter stream, with the hashtag below
    // and saves them as a text file
    System.setProperty("twitter4j.oauth.consumerKey", sys.env("TWITTER_CONSUMER_KEY"))
    System.setProperty("twitter4j.oauth.consumerSecret", sys.env("TWITTER_CONSUMER_SECRET"))
    System.setProperty("twitter4j.oauth.accessToken", sys.env("TWITTER_ACCESS_TOKEN"))
    System.setProperty("twitter4j.oauth.accessTokenSecret", sys.env("TWITTER_ACCESS_SECRET"))

    val config = new SparkConf().setMaster("local[*]").setAppName("stock-guru")
    val sc = new SparkContext(config)
    val tweetsCollected = sc.accumulator(0)
    val tweetsToCollect = 100

    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Seconds(1))
    val stream = TwitterUtils.createStream(ssc, None)

    val compAndTags = TwitterAnalysisUtils.getTags();

    stream.foreachRDD{(rdd, time) =>
      // uncomment to limit the number of tweets colected
      //tweetsCollected += rdd.count().toInt
      val newRdd = rdd.filter(t => t.getLang == "en")
        .filter(t => TwitterAnalysisUtils.checkCompanies(compAndTags,t.getText))
        .map( t =>{
        Map(
          "user"-> t.getUser.getScreenName,
          "created_at" -> t.getCreatedAt.toInstant.toString,
          "location" -> Option(t.getGeoLocation).map(geo => { s"${geo.getLatitude},${geo.getLongitude}" }),
          "text" -> t.getText,
          "hashtags" -> t.getHashtagEntities.map(_.getText),
          "retweet" -> t.getRetweetCount,
          "companies_mentioned" -> TwitterAnalysisUtils.companiesMentioned(compAndTags,t.getText),
          "text_sentiment_score" -> SentimentAnalysisUtils.detectSentiment(t.getText)
        )
      })

    // val filteredRdd = newRdd.filter( t => !(t("companies_mentioned").isEmpty))

    newRdd.saveToEs("tweets/tweet")

    newRdd.foreach{t =>
                print("User: ")
                println(t("user"))
                print("Text: ")
                println(t("text"))
                print("Companies mentioned :")
                println(t("companies_mentioned"))
                print("Text sentiment: ")
                println(t("text_sentiment_score"))

              }

      if (tweetsCollected.value >= tweetsToCollect) {
        System.exit(0)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
