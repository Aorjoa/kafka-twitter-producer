import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

import scala.collection.JavaConverters._
import twitter4j._
import twitter4j.TwitterFactory
import twitter4j.conf.ConfigurationBuilder

object ProducerObj {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Producer").setMaster("local[*]")
    val sc = new SparkContext(conf)
    // Configuration for Kafka brokers
    val kafkaBrokers = "localhost:9092"
    val topicName = "spark-test"

    val props = new Properties()
    props.put("bootstrap.servers", kafkaBrokers)
    props.put("acks", "all")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    def sendEvent(message: String) = {
      val key = java.util.UUID.randomUUID().toString()
      producer.send(new ProducerRecord[String, String](topicName, key, message))
      System.out.println("Sent event with key: '" + key + "' and message: '" + message + "'\n")
    }

    val twitterConsumerKey = ""
    val twitterConsumerSecret = ""
    val twitterOauthAccessToken = ""
    val twitterOauthTokenSecret = ""


    val cb = new ConfigurationBuilder()
    cb.setDebugEnabled(true)
      .setOAuthConsumerKey(twitterConsumerKey)
      .setOAuthConsumerSecret(twitterConsumerSecret)
      .setOAuthAccessToken(twitterOauthAccessToken)
      .setOAuthAccessTokenSecret(twitterOauthTokenSecret)

    val twitterFactory = new TwitterFactory(cb.build())
    val twitter = twitterFactory.getInstance()

    val query = new Query(" #เลื่อนแม่มึงสิ ")
    query.setCount(100)
    query.lang("en")
    var finished = false
    while (!finished) {
      val result = twitter.search(query)
      val statuses = result.getTweets()
      var lowestStatusId = Long.MaxValue
      for (status <- statuses.asScala) {
        if (!status.isRetweet()) {
          sendEvent(status.getText())
          Thread.sleep(60100)
        }
        lowestStatusId = Math.min(status.getId(), lowestStatusId)
      }
      query.setMaxId(lowestStatusId - 1)
    }
  }
}