
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.twitter._
import twitter4j.Status

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import scala.util.Random



object TwitterStreaming {


  def main(args: Array[String]): Unit = {


    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }

    var N = 0
    var sample_size = 100
    var tweets_sample = new ListBuffer[Status]()
    var tweets_sample_length = 0
    var hashtag_map = Map[String,Int]()


    val consumerKey = "wTuz7TjUIeEEy7BWkFBXf95JY".toString
    val consumerSecret = "JuYzX0Wv6EVnE2ZwcGgx4QCuUyQarxWDsfueU04v8TMxVEx73y".toString
    val accessToken = "2534458700-3G21t9iKrOSNgwwBSxFufjsi4vBRgcejVNgpZLB".toString
    val accessTokenSecret = "ksEMo3uter7A0xiiWAisHUyH5w4yKHT0hEQYI2c3waDhG".toString

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val spark_config = new SparkConf().setAppName("Homework5").setMaster("local[2]")
    val spark_context = new SparkContext(spark_config)
    spark_context.setLogLevel(logLevel = "OFF")

    val ssc = new StreamingContext(spark_context, Seconds(10))

    val stream = TwitterUtils.createStream(ssc, None)


    stream.foreachRDD(tweets => {

      val tweets_group = tweets.collect()
      // Array[status]


      for(tweet <- tweets_group){

        if(N < sample_size){
          /* append to the sample collection stream */
          tweets_sample.append(tweet)
          tweets_sample_length = tweets_sample_length + tweet.getText().length

          val hashtags = tweet.getHashtagEntities().map(_.getText)
          for(tag <- hashtags){
            if(hashtag_map.contains(tag)) hashtag_map(tag) += 1
            else hashtag_map(tag) = 1
          }

        }else{
          /* Get the stats here */
          /// since the sample is full now , get a random number from 1 to 100 and replace it with new tweet

          var r = Random.nextInt(N)

          if(r < sample_size){

            val tweet_to_remove = tweets_sample(r)
            tweets_sample(r) = tweet

            tweets_sample_length  = tweets_sample_length - tweet_to_remove.getText.length + tweet.getText().length

            /* Remove old tweet's hashtags */
            val hashtags = tweet_to_remove.getHashtagEntities().map(_.getText)
            for(tag <- hashtags){
              hashtag_map(tag) -= 1
            }

            /* Add new tweet's hashtags */
            val hashtags2 = tweet.getHashtagEntities().map(_.getText)
            for(tag <- hashtags2) {
              if(hashtag_map.contains(tag)) hashtag_map(tag) += 1
              else hashtag_map(tag) = 1
            }


            val size = hashtag_map.toSeq.sortBy{case(tweet,count) => -count}.take(5)

            println("The number of the twitter from beginning: " + (N + 1))
            println("Top 5 hot hashtags:")

            for(i <-  size){

              if(i._2 != 0) {
                println(i._1 + ":" + i._2)
              }
            }

            var avg_tweetlenth = tweets_sample_length/(sample_size.toFloat)
            println("The average length of the twitter is: " +  avg_tweetlenth)
            println("\n\n")

          }

        }
        N = N+1
      }

    })
    ssc.start()
    ssc.awaitTermination()
  }

}