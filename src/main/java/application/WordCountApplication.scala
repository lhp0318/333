package application

import common.CommonApplication
import controller.WordCountController

/**
  * WordCount 应用程序
  */
object WordCountApplication extends CommonApplication with App{

    startApp(appName="WordCount") {
        val controller = new WordCountController()
        controller.execute()
    }

}
