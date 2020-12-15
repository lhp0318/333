import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.{BufferedSource, Source}
import scala.util.Random

object test {
  def main(args: Array[String]): Unit = {

    //    System.setProperty("HADOOP_USER_NAME", "atguigu")
    //    val spark: SparkSession = SparkSession
    //      .builder()
    //      .enableHiveSupport()
    //      .master("local[*]")
    //      .appName("sql")
    //      .getOrCreate() 只限制了数据容纳的个数 没有限定数据容纳的类型
    //val t: (Int, String, String) = (1,"123","a")
    //    println(t._1)
    //    println(t._2)
    //    println(t._3)
    //    如果元祖中只有两个元素 name这样的元祖称之为对偶元祖 也称之为 键值对
    //    val kv = ("a", 1)
    //    //    println(t)
    //    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ReceiverWordCount")
    //    val ssc = new StreamingContext(sparkConf,Seconds(3))
    //    val lineStreams = ssc.socketTextStream("localhost",9999)
    //    val wordStreams = lineStreams.flatMap(_.split(" "))
    //    val wordAndOneStreams = wordStreams.map((_,1))
    //    val wordAndCountStreams=wordAndOneStreams.reduceByKey(_+_)
    //    wordAndCountStreams.print()
    //    ssc.start()
    //    ssc.awaitTermination()
    //    val sparkConf:SparkConf = new SparkConf().setMaster("local[*]").setAppName("ReceiverWordCount")
    //    val ssc = new StreamingContext(sparkConf,Seconds(3))
    //    val kafkaPara:Map[String,Object]=Map[String,Object](
    //      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
    //      ConsumerConfig.GROUP_ID_CONFIG -> "atguigu",
    //      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    //      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    //    )
    //    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
    //      LocationStrategies.PreferConsistent,
    //      ConsumerStrategies.Subscribe[String, String](Set("test"), kafkaPara))
    //
    //    //5.将每条消息的KV取出
    //    val valueDStream: DStream[String] = kafkaDStream.map(record => record.value())
    //
    //    //6.计算WordCount
    //    valueDStream.flatMap(_.split(" "))
    //      .map((_, 1))
    //      .reduceByKey(_ + _)
    //      .print()
    //
    //    //7.开启任务
    //    ssc.start()
    //    ssc.awaitTermination()
    // val sparkConf:SparkConf =new SparkConf().setAppName(
    //      "ReceiverWORDcount"
    //    ).setAppName("local[*]")
    //    val ssc = new StreamingContext(sparkConf,Seconds(3))
    //    val kafkaPara:Map[String,Object]=Map[String,Object](
    //
    //    )

    //
    //    val producer = new KafkaProducer[String, String]()
    //    while(true){
    //
    //
    //      val record = new ProducerRecord[String,String](topic,dataString)
    //      producer.send(record)
    //      Thread.sleep(2000)
    //    }
    //    def mockData()={
    //      val topic = "atguigu200820"
    //      val areas=List("华北","花乃","华中")
    //      val cities=List("bj","sh","sz")
    //
    //      val list = ListBuffer[String]()
    //      for(i<- 1 to 50){
    //        val area = areas(new Random().nextInt(3))
    //        val city = cities(new Random().nextInt(3))
    //        val userid=new Random().nextInt(6)+1
    //        val adid = new Random().nextInt(6)+1
    //        val dataString = s"${System.currentTimeMillis() }${area} ${city}${userid}${adid}"
    //        list.append(dataString)
    //      }
    //      list
    //    }
    /*
    读取数据文件 将读取的每一行字符串拆分成每一个单词 将相同的单词分在一个组中
    map(word->list(word,word,word))
    将相同的单词统计出现的数量
    list(word,wrod,word)=>3
    map(word->3)
    将统计的结果进行排序 降序
    将结果取前三名
     */
    //    val in: BufferedSource = Source.fromFile("input/word.txt")
    //    val list: List[String] = in.getLines().toList
    //    val word: List[String] = list.flatMap(
    //      line => line.split(" ")
    //    )
    //    val wordTOList: Map[String, List[String]] = word.groupBy(word => word)
    //    val stringToInt: Map[String, Int] = wordTOList.map(t => {
    //      val w = t._1
    //      val c = t._2
    //      (w, c.size)
    //    })
    //    println(stringToInt.toList.sortBy(_._2)(Ordering.Int.reverse).take(3))
    //
    //    in.close()
    //    list.map(
    //      t=>{
    //val newLIST: Unit = {
    //        val key =(t._1+" ")*t._2
    //
    //      }
    //    })
//    val list = List(
//      ("hello spark scala",4), ("hello spark",3), ("hello",2)
//    )
//    val tuples: List[(String, Int)] = list.flatMap(
//      t => {
//        val line = t._1
//        val cnt = t._2
//        val words = line.split(" ")
//        words.map(
//          word =>
//            (word, cnt)
//        )
//      }
//    )
//    println(tuples)
//    val stringToTuples: Map[String, List[(String, Int)]] = tuples.groupBy(_._1)
//    println(stringToTuples)
//    val stringToInt: Map[String, Int] = stringToTuples.map(
//      t => {
//        val word = t._1
//        val list = t._2
//        val newLIST = list.map(_._2)
//        val sum = newLIST.sum
//        (word, sum)
//      }
//    )
//    println(stringToInt)

  }
}
