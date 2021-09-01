package cn.doitedu.spark.demos

import org.apache.spark.{SparkConf, SparkContext}

object HelloWorld {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("分区测试")
    conf.set("spark.default.parallelism", "5")
    val sc = new SparkContext(conf)

    val rdd0  = sc.makeRDD((1 to 10000).zip(10000 to 20000), 10000)
    val rdd1  = rdd0.reduceByKey(_+_,5)   // HashPartitioner(400)

    rdd1.count

  }
}
