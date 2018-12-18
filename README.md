# Mining-Datastream-Algorithms

## Objective 
Implement streaming algorithms on Twitter stream.


## Environment Requirements
- Scala 2.11 version
- Spark 2.2.1 version


## Dataset
Realtime Twitter data stream 

## Reservoir Sampling Algorithm 
I have collected (continuously) the stream of data (tweets) for a time period of 10 seconds in an RDD.
Now, for every RDD, I have collected the sample of tweets one by one upto 100 and once the sample size reaches 100, for every new tweet , I performed two steps:

```
1. I calculated a random number between 0 to 99 and replaced that numbered tweet from the sample size with the new tweet
2. Now, for calculating the popular tags, I have maintained a map which keeps count of each tag. I sorted this map and printed the top 5 tags Calculated the top 5 popular hash tags from these sample of tweets and printed them in the terminal.
```


## BloomFiltering Algorithm
I have collected (continuously) the stream of data (tweets) for a time period of 10 seconds in an RDD. Now, for every RDD, I have maintained a hash map which acts like a bit vector for bloom filter in my code. I have used two hash functions : fnv hash and bp hash . I have also maintained tags set collected in the stream for calculation of false positive. The Code algorithm is as follows:

```
1. For set of tags (for each tweet ), I have generated hash codes from fnv hash and bp hash , and checked if these two codes have been hashed to 1 , if both of them hashcodes have 1 as their map values, then that means , this tag could “probably be” in the sample of tags collected previously seen. Now, I check the tags set previously seen and validate if this a false positive or a value previously seen in the stream( true positive)
2. If any one of the hashcode doesn’t have 1 as their value, then it means , this tag is “definitely not” seen previously .
3. After parsing all the tweets, I have calculated the statistics : false positives (values which yield 1 in the hash map(bloom filter bit vector) but are not previously seen ), correct estimates (total number of tags estimated correctly in the stream , which is tag set in my code), percentage of false positives .
```


NOTE: for my code , I have calculated the statistics based only on this stream of data , because I felt , if the tag set is maintained from the beginning storage would be a concern ? (how many of the tags to be maintained and till what capacity ) , and also , sample is collected only for a short time of 10 sec, which is why most of the false positive estimates would turn out to be zero , This only means that the hash functions I have used are working perfect . But if the time limit for collecting stream is increased

## Command to Run the code 
For Reservoir Sampling :

```
spark-submit --class Niharika_Gajam_TwitterStreaming Niharika_Gajam_HW5.jar
```

For BloomFiltering :

``` 
spark-submit --class Niharika_Gajam_BloomFiltering Niharika_Gajam_HW5.jar
```
