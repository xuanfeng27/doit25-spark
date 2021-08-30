package cn.doitedu.spark.demos

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkConf, SparkContext}

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-08-30     
 * @desc 分区数的决定机制：
 *  源头RDD的分区数，由数据源的相关特性决定
 *  后续的窄依赖RDD （map/flatmap/mappartitions/filter/mapvalues），分区数通常是一路传承不改变的
 *  后续的宽依赖RDD ，分区数是由shuffle算子(reduceByKey(f,4)/groupBy()/groupByKey()/cogoupByKey()/join)传入分区数来决定的
 *     如果没有传入，则由spark.default.parallelism参数决定
 *     如果参数也没有设置，则由上游RDD中的最大分区数决定
 *
 */


object C09_RDD的分区和分区器 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("分区测试")
    conf.set("spark.default.parallelism", "8")
    val sc = new SparkContext(conf)

    //
    val rddFile: RDD[String] = sc.textFile("data/input")
    println(rddFile.partitions.size)
    println(rddFile.partitioner)



    // local模式下，默认并行度：defaultParallelism()=scheduler.conf.getInt("spark.default.parallelism", totalCores)
    // 分布式模式下，默认并行度： conf.getInt("spark.default.parallelism", math.max(totalCoreCount.get(), 2))
    val rdd1: RDD[Int] = sc.makeRDD(1 to 10000,5)
    val partitions: Array[Partition] = rdd1.partitions
    println(partitions.size)
    println(rdd1.partitioner)
    println("============== 多易教育 ==================")

    val rdd2: RDD[Int] = rdd1.map(_ * 10)
    println(rdd2.partitions.size)
    println(rdd2.partitioner)
    println("============== 多易教育 ==================")

    val rdd3: RDD[Int] = rdd1.flatMap(i => 0 to i)
    println(rdd3.partitions.size)
    println(rdd3.partitioner)
    println("============== 多易教育 ==================")

    val rdd4: RDD[Int] = rdd3.filter(_ % 2 == 0)
    println(rdd4.partitions.size)
    println(rdd4.partitioner)
    println("============== 多易教育 ==================")

    /**
     * shuffle算子，通常允许用户传入分区数或分区器
     * 说明，shuffle是允许改变分区（分区的规则和数量）的
     */
    val rdd5: RDD[(Int, Int)] = rdd1.map((_, 10))
    val rdd6: RDD[(Int, Int)] = rdd5.reduceByKey(_ + _, 2)
    println(rdd6.partitions.size)  // 2
    println(rdd6.partitioner)
    println("============== 多易教育 ==================")



    val rdd7 = rdd5.groupByKey(4).mapValues(_.sum)
    println(rdd7.partitions.size)  // 4
    println(rdd7.partitioner)
    println("============== 多易教育 ==================")

    //我们会使用参数spark.default.parallelism的值作为默认的分区数
    //如果参数没有设置，会用上游 RDD的最大分区数
    val rdd8: RDD[(Int, Int)] = rdd5.reduceByKey(_ + _)
    println(rdd8.partitions.size)   // ?
    println(rdd8.partitioner)

    println("============== 多易教育 ==================")

    val rddx = sc.makeRDD(1 to 10000,5).map((_,10))
    val rddy = sc.makeRDD(1 to 10000,25).map((_,20))
    val rddz = rddx.join(rddy)
    println(rddz.partitions.size)
    println(rddz.partitioner)


    println("============== 多易教育 ==================")

    sc.stop()
  }

}
