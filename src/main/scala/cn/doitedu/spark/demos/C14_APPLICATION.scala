package cn.doitedu.spark.demos

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object C14_APPLICATION {
  def main(args: Array[String]): Unit = {

    val conf1 = new SparkConf().setAppName("application_第一个").setMaster("local")
    // 一个sparkcontext ，就是一个application
    val sc1 = new SparkContext(conf1)


    val conf2 = new SparkConf().setAppName("application_第二个").setMaster("local")
    // 一个sparkcontext ，就是一个application
    val sc2 = new SparkContext(conf2)


    // 触发一个行动算子，就触发了一个job
    val rdd1: RDD[Int] = sc1.makeRDD(1 to 100000, 4)
    rdd1.reduce(_ + _)  // action算子，触发了 job1
    rdd1.map((_,10)).reduceByKey(_+_).saveAsTextFile("data/xxout")  //action算子，触发了job2


  }

}
