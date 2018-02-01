import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object SparkReceiver {
  val topic = "test-1"
  val broker = "localhost:9092"

  def getDataFrame(spark: SparkSession,stream: DataFrame,schema: StructType) ={
    import spark.implicits._
    val data = stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
              .as[(String, String)].map(_._2)
    data.select(from_json($"value",schema).as("tweet")).select($"tweet.*")
  }

  def buildPipeline(tweet: DataFrame) = {
     tweet.filter(size(col("entities.hashtags")) > 0).withColumn("hashs", explode(col("entities.hashtags")))
                .filter(col("hashs").isNotNull && col("user.location").isNotNull && col("retweeted")
                .contains("false")).select(col("id"),col("user.name"),col("user.location"),col("geo"),
                col("hashs.text").alias("hashtags"),col("retweeted"),col("retweet_count"),col("text")
                .alias("tweet"),hour(to_timestamp(col("created_at"),"EEE MMM dd HH:mm:ss Z yyyy"))
                .alias("hr")).groupBy("hr","location").count()
                /*.groupBy("id","created_at","name","location",
                  "geo","retweeted","retweet_count","tweet").agg(collect_list("hashtags").as("hashtags"))
                  */
  }

  def main(args: Array[String]) = {
    val spark = SparkSession.builder
                .master("spark://sachin-GA-880GM-USB3:7077")
                .appName("SparkDataProcessor")
                .getOrCreate()
    val schema = spark.read.json("test.json").schema
    val stream = spark
                .readStream
                .format("kafka")
                .option("kafka.bootstrap.servers",broker)
                .option("subscribe", topic)
                .load()
    val tweet = getDataFrame(spark,stream,schema)
    val query = buildPipeline(tweet).writeStream
                .outputMode("complete")
                .format("console")
                .start()

    query.awaitTermination()

  }
}
