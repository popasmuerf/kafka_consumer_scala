import java.util
//import java.util.Set
//import java.util.HashSet
import java.util.HashMap
import java.util.Properties

import com.sun.org.apache.bcel.internal.classfile.LineNumber
import io.netty.handler.codec.string.StringDecoder

import scala.util.matching.Regex
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.streaming.kafka.KafkaUtils



/**
  * Created by mikeyb on 7/30/17.
  * https://jaceklaskowski.gitbooks.io/spark-streaming/spark-streaming-kafka.html
  * <zkQourum> is alist of one or more zookeeper servers that make a quorum
  * <group> is the name of kafka consumer group
  * <topics> is a list of one or more kafka tioics to consume from <numThreads> is the
  * number of threads the kafka cosnumer should use
  */
object Driver {
  val zkQuorum = "zookp1"
  val group = "pfsenseGroup1"
  val topics = "pfsense"
  val topic = "pfsense"
  val numThreads = "1"
  def main(args:Array[String]): Unit ={
    val args = Array(zkQuorum,group,topics,numThreads)
    val sparkMaster = "local[*]"
    val sparkAppName = "etl_pfsense test driver"
    val sparkConf = new SparkConf()
    sparkConf.setAppName(sparkAppName)
    sparkConf.setMaster(sparkMaster)
    sparkConf.set("spark.streaming.backpressure.enabled","true")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    ssc.checkpoint("/tmp/checkpoint")

    import org.apache.log4j._
    Logger.getLogger("org.apache.spark.streaming.dstream.DStream").setLevel(Level.DEBUG)
    Logger.getLogger("org.apache.spark.streaming.dstream.WindowedDStream").setLevel(Level.DEBUG)
    Logger.getLogger("org.apache.spark.streaming.DStreamGraph").setLevel(Level.DEBUG)
    Logger.getLogger("org.apache.spark.streaming.scheduler.JobGenerator").setLevel(Level.DEBUG)

    import _root_.kafka.serializer.StringDecoder
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    val kafkaTopics = Set(topic)
    val records = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, kafkaTopics)
    records.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
