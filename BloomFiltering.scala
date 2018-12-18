
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.twitter._
import twitter4j.Status

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import scala.util.Random



object BloomFiltering{


  def FNVHash(str: String): Long = {
    val fnv_prime = 0x811C9DC5
    var hash = 0
    var i = 0
    while ({i < str.length}) {
      hash *= fnv_prime
      hash ^= str.charAt(i)

      {
        i += 1; i - 1
      }
    }

    hash
  }

  def BPHash(str: String): Long = {
    var hash = 0
    var i = 0
    while ({i < str.length}){
      hash = hash << 7 ^ str.charAt(i)

      {
        i += 1; i - 1
      }
    }
    hash
  }

  def main(args: Array[String]): Unit = {


    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }

    val consumerKey = "wTuz7TjUIeEEy7BWkFBXf95JY"
    val consumerSecret = "JuYzX0Wv6EVnE2ZwcGgx4QCuUyQarxWDsfueU04v8TMxVEx73y"
    val accessToken = "2534458700-3G21t9iKrOSNgwwBSxFufjsi4vBRgcejVNgpZLB"
    val accessTokenSecret = "ksEMo3uter7A0xiiWAisHUyH5w4yKHT0hEQYI2c3waDhG"

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)



    val spark_config = new SparkConf().setAppName("Homework5").setMaster("local[2]")
    val spark_context = new SparkContext(spark_config)

    spark_context.setLogLevel(logLevel = "OFF")


    val ssc = new StreamingContext(spark_context, Seconds(10))

    val stream = TwitterUtils.createStream(ssc, None)

    var right_estimates = 0
    /* Bloom Filtering Algorithm */
    stream.foreachRDD(tweets => {

      var hashtag_map = Map[Long,Int]()   // our bloom filter !!!!
      val tweets_group = tweets.collect()
      var tag_set : Set[String] = Set()  // Actual set of tags


      var false_positive_count = 0

      var count = 1

      // want the unique tag set count which is the right estimates

      for(tweet <- tweets_group){

        val hashtags = tweet.getHashtagEntities().map(_.getText)

        for(tag <- hashtags){

          /* hash the tag using different hashes */
          var fnv = FNVHash(tag)
          var bp = BPHash(tag)


          if( hashtag_map.contains(fnv) && hashtag_map.contains(bp)){
            // if( hashtag_map.contains(fnv)){
            // not in the bloom filter already  then add to the bloom filter
            // check for false positivity
            if(!tag_set.contains(tag)){ // falsepositive
              false_positive_count = false_positive_count + 1
            }
          }


          /* updating the bloom filters */
          if(hashtag_map.contains(fnv)) hashtag_map.update(fnv,1)
          else hashtag_map(fnv) = 1

          if(hashtag_map.contains(bp)) hashtag_map.update(bp,1)
          else hashtag_map(bp) = 1


          if(tag_set.isEmpty){
            tag_set = tag_set + tag
          }else if(!tag_set.contains(tag)){ // if its not in the tag set before
            tag_set = tag_set + tag
          }

        }

        count = count + 1   // count of tweets
      }


      right_estimates = tag_set.size
      /* statistics for the bloom filter */

      //println("Total tweets in the stream of 10 seconds : " + count)
      println("Right estimates in the stream : " + right_estimates)
      println("False positives or Wrong Estimates in the stream : " + false_positive_count)
      // println(" bloom filter size" + hashtag_map.size)

      if(right_estimates!=0){
        var percent_fp = false_positive_count/right_estimates * 100
        println("% of false positives " + percent_fp + "%")
      }

    })



    ssc.start()
    ssc.awaitTermination()
  }

}