package controller

import common.CommonController
import service.WordCountService

/**
  * WordCount控制器
  */
class WordCountController extends CommonController {
    private val wordCountService = new WordCountService()

    // 所有的控制器都应该有execute方法
    override def execute(): Unit = {
        val wordToSum = wordCountService.analysis()
        wordToSum.foreach(println)
    }
}
