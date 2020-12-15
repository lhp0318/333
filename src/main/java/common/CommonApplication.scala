package common

import util.EnvUtil

trait CommonApplication {

    // 函数 ： 控制抽象
    // 函数 ：柯里化
    // 参数默认值
    // 隐式转换
    protected def startApp(master:String="local[*]", appName:String)( f : => Unit ): Unit = {
        implicit val name : String = appName
        EnvUtil.putEnv(master)

        try {
            f
        } catch {
            case e:Exception => println(e.getMessage)
        }
        EnvUtil.closeEnv()
    }
}
