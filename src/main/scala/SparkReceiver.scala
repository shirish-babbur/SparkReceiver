import org.apache.spark.SparkConf
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka._
import org.apache.spark.sql._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.types._

object SparkReceiver {
  val topic = "test-1"
  val broker = "localhost:9092"

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("SparkReceiver").setMaster("local[2]")
    val context = new StreamingContext(conf,Seconds(2))
    val kafkaParam = Map[String,String]("metadata.broker.list" -> broker)
    val kafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](context,kafkaParam,topic.split(",").toSet).map(_._2)

    kafkaStream.foreachRDD {
      rdd =>
      if(rdd.toLocalIterator.nonEmpty){
        val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
        val sqlContext = new SQLContext(context.sparkContext)
        import spark.implicits._
        val tweet = sqlContext.jsonRDD(rdd)
        val df = tweet.toDF()
        val keys = udf[Seq[String], Map[String, Row]](_.keys.toSeq)
        println("******************")
        df.withColumn("hashs", explode($"entities.hashtags")).printSchema
        println("******************")
        val tags = df.withColumn("hashs", explode($"entities.hashtags")).filter($"hashs".isNotNull)
                    .select("id","timestamp_ms","user.name","geo","hashs.text").groupBy("id","timestamp_ms","name","geo")
                    .agg(collect_list("text").as("hashtags"))
        val text = df.select("id","text")
        val tweetdf = tags.join(text,"id")
        tweetdf.show()
        //data.saveToCassandra("test","data",SomeColumns("key","value"))
      }
    }
    context.start()
    context.awaitTermination()
  }
}
