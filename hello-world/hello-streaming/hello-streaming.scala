import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._


object TwitterStream {
  def main(args: Array[String]){
    
    // Collect 10 tweets from the current twitter stream, with the hashtag below
    // and saves them as a text file

    var numTweetsCollected = 0L
    val numTweetsToCollect = 10
    val hashtag = "nowplaying"

    System.setProperty("twitter4j.oauth.consumerKey", sys.env("TWITTER_CONSUMER_KEY"))
    System.setProperty("twitter4j.oauth.consumerSecret", sys.env("TWITTER_CONSUMER_SECRET"))
    System.setProperty("twitter4j.oauth.accessToken", sys.env("TWITTER_ACCESS_TOKEN"))
    System.setProperty("twitter4j.oauth.accessTokenSecret", sys.env("TWITTER_ACCESS_SECRET"))

    val config = new SparkConf().setMaster("local[2]").setAppName("twitter-stream")
    val sc = new SparkContext(config)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(5))
    val stream = TwitterUtils.createStream(ssc, None)
    val tweets = stream.filter {t => val tags = t.getText.split(" ").filter(_.startsWith("#")).map(_.toLowerCase)
      tags.contains(hashtag)}

    tweets.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (count > 0) {
        println("Tweet encontrado")
        rdd.saveAsTextFile("results/tweets_" + time.milliseconds.toString)
        numTweetsCollected += count
        if (numTweetsCollected > numTweetsToCollect) {
          System.exit(0)
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
