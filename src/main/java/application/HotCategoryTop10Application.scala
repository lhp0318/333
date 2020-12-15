package application

import common.CommonApplication
import controller.HotCategoryTop10Controller

object HotCategoryTop10Application extends CommonApplication with App {

    startApp(appName="HotCategoryTop10") {
        val controller = new HotCategoryTop10Controller
        controller.execute()
    }
}
