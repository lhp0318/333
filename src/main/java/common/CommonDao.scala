package common

import org.apache.spark.rdd.RDD
import util.EnvUtil

trait CommonDao {

    def getFileData(path:String) = {
        val lines: RDD[String] = EnvUtil.getEnv().textFile(path)
        lines
    }
    def getMemoryData() = {
        EnvUtil.getEnv().makeRDD(List(1,2,3,4))
    }
}
