package cn.doitedu.spark.demos

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-08-24
 * @desc 深似海男人的一个深似海的程序
 */
object C01_入门程序WordCount {

  def main(args: Array[String]): Unit = {
    // 构造一个spark的参数配置对象
    val conf = new SparkConf()
    //conf.setMaster("local") // 以单线程运行; local[*]以机器上的cpu线程数来作为并发数; local[2]以指定的数字的线程数作为并发数;
    conf.setAppName("深似海男人的一个深似海程序")

    // 构造一个spark的driver工具
    val sc: SparkContext = new SparkContext(conf)

    // 1.加载数据（读数据）
    val rdd1: RDD[String] = sc.textFile("hdfs://doit01:8020/sparktest/wordcount/input") // a a a a a b c

    // 2.写计算逻辑
    /*val rdd2: RDD[String] = rdd1.flatMap(s => s.split("\\s+")) // a
    val rdd3: RDD[(String, Iterable[String])] = rdd2.groupBy(x => x)
    val rdd4: RDD[(String, Int)] = rdd3.mapValues(iter => iter.size)*/

    val rdd5: RDD[(String, Int)] = rdd1.flatMap(s => s.split("\\s+")).map(s => (s, 1)).reduceByKey(_ + _)

    // 3.输出结果
    //rdd4.foreach(println)
    rdd5.saveAsTextFile("hdfs://doit01:8020/sparktest/wordcount/output")

    sc.stop()
  }

}