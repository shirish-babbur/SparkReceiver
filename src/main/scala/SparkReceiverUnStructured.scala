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

object SparkReceiverUnstructured {
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
    df.filter(size(col("entities.hashtags")) > 0).withColumn("hashs", explode(col("entities.hashtags")))
                .filter(col("hashs").isNotNull && col("user.location").isNotNull && col("user.location")
                .contains("India")).select(col("id"),col("user.name"),col("user.location"),col("geo"),
                col("hashs.text").alias("hashtags"),col("text").alias("tweet"),col("created_at"))
                .groupBy("id","created_at","name","location","geo","tweet")
                .agg(collect_list("hashtags").as("hashtags"))
  }

  def main(args: Array[String])={
  val conf = new SparkConf().setAppName("SparkReceiver").setMaster("local[2]")
  val context = new StreamingContext(conf,Seconds(2))
  val kafkaStream = prepareStream(context)

  kafkaStream.foreachRDD {
    rdd =>
    try{
      if(rdd.toLocalIterator.nonEmpty){
        var sqlContext = getSQLContext(rdd,context)
        val df = getDataFrame(rdd,sqlContext)
        df.show()
        df.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "tweets","keyspace" -> "twitter",
          "cluster" -> "Test Cluster")).mode(SaveMode.Append)
          .save()
      }
    }catch {
      case e:Throwable =>{
        println(e)
      }
    }
  }

  context.start()
  context.awaitTermination()
}
