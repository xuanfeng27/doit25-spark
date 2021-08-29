package cn.doitedu.spark.demos

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object C07_SPARK执行流程 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("SPARK执行流程")
    val sc = new SparkContext(conf)


    val rdd1: RDD[String] = sc.textFile("data/wordcount.txt")


    val rdd2: RDD[String] = rdd1.flatMap(_.split("\\s+"))


    val rdd3: RDD[(String, Int)] = rdd2.map((_, 1))


    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey(_ + _,2)


    val rdd5: RDD[(String, Int)] = rdd4.map(tp => (tp._1, tp._2 * 10))


    val rdd6: RDD[(String, Iterable[Int])] = rdd5.groupByKey(3)


    val rdd7: RDD[(String, Int)] = rdd6.mapValues(_.size)


    rdd7.saveAsTextFile("dataout/xx")


    rdd7.count()

    Thread.sleep(Long.MaxValue)


    sc.stop()


  }

}

















