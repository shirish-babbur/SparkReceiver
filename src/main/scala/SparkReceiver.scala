import org.apache.spark.SparkConf
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka._
import org.apache.spark.sql._


object SparkReceiver {
  val topic = "test-1"
  val broker = "localhost:9092"

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("SparkReceiver").setMaster("local[2]")
    val context = new StreamingContext(conf,Seconds(2))
    val kafkaParam = Map[String,String]("metadata.broker.list" -> broker)
    val kafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](context,kafkaParam,topic.split(",").toSet)
    println("Just before streaming--------------->")
    //kafkaStream.print(50)
    //val x = kafkaStream.map(_._2)
    //x.print(100)

    kafkaStream.foreachRDD {
      rdd =>
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      val df = spark.createDataFrame(rdd)
      df.createOrReplaceTempView("tweets")
      val tweetData = spark.sql("select * from the tweets")
      tweetData.show()
    }
    context.start()
    context.awaitTermination()
  }
}
