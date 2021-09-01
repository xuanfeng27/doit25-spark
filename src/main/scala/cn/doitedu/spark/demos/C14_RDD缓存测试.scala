package cn.doitedu.spark.demos

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object C14_RDD缓存测试 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("缓存测试")
    val sc = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(1 to 10000000, 3)

    rdd1.cache()

    val res1: Long = rdd1.count()
    val res2: Array[(Int, Int)] = rdd1.map((_, 100)).reduceByKey(_ + _).take(10)

    Thread.sleep(60000)

    rdd1.unpersist(true)

    println("缓存已经清除完毕")


    Thread.sleep(Long.MaxValue)
    sc.stop()

  }
}
