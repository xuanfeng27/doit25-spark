package cn.doitedu.spark.demos

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/***
 * 当使用shuffle算子，并且没有传入分区数，也没有传入分区器，则会调用如下方法去获取一个默认的分区器：
 * object Partitioner.defaultPartitioner()
 */
object C10_默认分区器创建规则 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("分区测试")
    conf.set("spark.default.parallelism", "500")
    val sc = new SparkContext(conf)

    val rdd0  = sc.makeRDD((1 to 10000).zip(10000 to 20000), 10000)
    val rdd1  = rdd0.reduceByKey(_+_,400)   // HashPartitioner(400)
    val rdd2  = rdd0.sortByKey(false,20)  // RangePartitioner(1000)

    val rdd3  = rdd1.join(rdd2)
    // 如果上游rdd中没有任何一个rdd拥有分区器，则直接获取一个新的hashPartitioner
    //     上游rdd的最大分区数是： 10000
    //     上游rdd的最大分区器是：rdd1的分区器（400）
    //     默认并行度：8000
    //     是否选用上游rdd的分区器，取决于如下条件：
    //        1.  最大分区器的分区数 > 默认并行度  ×！！ 不满足
    //        2.  最大分区器是否eligible :   上游最大分区数 / 最大分区器的分区数  < 10倍    ×！！ 不满足
    //     上面的条件不满足，则会new一个新的HashPartitioner
    val rdd4  = rdd1.cogroup(rdd2, rdd0)

    // rdd3的分区器是？  分区数？
    println(rdd3.partitioner)
    println(rdd3.partitions.size)


    // rdd4的分区器是？  分区数？
    println(rdd4.partitioner)
    println(rdd4.partitions.size)


    sc.stop()

  }

}
