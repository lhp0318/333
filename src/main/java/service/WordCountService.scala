package service

import common.CommonService
import dao.WordCountDao
import org.apache.spark.rdd.RDD

/**
  * WordCount服务对象
  */
class WordCountService extends CommonService{

    private val wordCountDao = new WordCountDao()
    /**
      * 数据分析
      */
    def analysis() = {
        val lines = wordCountDao.getFileData("input/word.txt")
        val words: RDD[String] = lines.flatMap(_.split(" "))
        val wordToOne = words.map(
            word => (word, 1)
        )
        val wordToSum = wordToOne.reduceByKey(_+_)
        wordToSum.collect()
    }
}
