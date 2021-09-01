package cn.doitedu.spark.demos

import org.apache.spark.{SparkConf, SparkContext}

object HelloWorld {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("分区测试")
    conf.set("spark.default.parallelism", "5")
    val sc = new SparkContext(conf)

    val rdd0 = sc.makeRDD((1 to 1000000).zip(1000000 to 2000000), 10)
    val rdd1 = rdd0.map(tp => (tp._1 + "aaaaaaaaaaaaaaaaaaaaaaaaaa", tp._2))

    rdd1.cache()

    val res = rdd1.reduceByKey(_ + _, 5) // HashPartitioner(400)

    res.count

    rdd1.map(tp=>(tp._1+"bbbbbbbbbb",tp._2)).count()

    Thread.sleep(Long.MaxValue)

    sc.stop()

  }
}
