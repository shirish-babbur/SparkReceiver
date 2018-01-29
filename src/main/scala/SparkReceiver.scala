import org.apache.spark.SparkConf
import kafka.serializer.StringDecoder
import org.apache.spark.rdd._
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

  def prepareStream(context: StreamingContext) = {
    val kafkaParam = Map[String,String]("metadata.broker.list" -> broker)
    KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
                      context,kafkaParam,topic.split(",").toSet).map(_._2)
  }

  def getSQLContext(rdd: RDD[String], context: StreamingContext) = {
    val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
    import spark.implicits._
    new SQLContext(context.sparkContext)
  }

  def getDataFrame(rdd: RDD[String],sqlContext: SQLContext) = {
    val tweet = sqlContext.jsonRDD(rdd)
    val df = tweet.toDF()
    val tags = df.filter(size(col("entities.hashtags")) > 0).withColumn("hashs", explode(col("entities.hashtags")))
                .filter(col("hashs").isNotNull).select("id","timestamp_ms","user.name","geo","hashs.text")
                .groupBy("id","timestamp_ms","name","geo").agg(collect_list("text").as("hashtags"))
    val text = df.select("id","text")
    tags.join(text,"id")
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("SparkReceiver").setMaster("local[2]")
    val context = new StreamingContext(conf,Seconds(2))
    val kafkaStream = prepareStream(context)

    kafkaStream.foreachRDD {
      rdd =>
      if(rdd.toLocalIterator.nonEmpty){
        var sqlContext = getSQLContext(rdd,context)
        val df = getDataFrame(rdd,sqlContext)
        df.show()
        //data.saveToCassandra("test","data",SomeColumns("key","value"))
      }
    }
    context.start()
    context.awaitTermination()
  }
}
