import java.text.SimpleDateFormat

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf


import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.kafka010.KafkaUtils._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object REQ {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val kafkaPara: Map[String, Object] =
      Map[String, Object](
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
        ConsumerConfig.GROUP_ID_CONFIG -> "atguigu",
        "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
        "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
      )
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      createDirectStream[String, String](ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Set("test"), kafkaPara))

    val kafkaVal: DStream[String] = kafkaDStream.map(_.value())

    val reduceDS: DStream[((String, String, String, String), Int)] = kafkaVal.map(
      line => {
        val datas = line.split(" ")
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val day = sdf.format(new java.util.Date(datas(0).toLong))
        val area = datas(1)
        val city = datas(2)
        val ad = datas(4)
        ((day, area, city, ad), 1)
      }
    ).reduceByKey(_ + _)
    reduceDS.foreachRDD(
      rdd=>{
        rdd.foreachPartition(
          iter=>{
            val conn=JdbcUtil
          }
        )
      }
    )

  }
}
