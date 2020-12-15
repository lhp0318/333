package controller

import common.CommonController
import service.HotCategoryTop10Service

/**
  * 热门品类Top10控制器
  */
class HotCategoryTop10Controller extends CommonController{

    private val hotCategoryTop10Service = new HotCategoryTop10Service

    override def execute(): Unit = {
        val result = hotCategoryTop10Service.analysis()
        result.foreach(println)
    }
}
