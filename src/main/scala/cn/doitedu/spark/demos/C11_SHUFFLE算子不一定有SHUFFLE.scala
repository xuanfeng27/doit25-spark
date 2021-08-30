package cn.doitedu.spark.demos

import org.apache.spark.{SparkConf, SparkContext}

object C11_SHUFFLE算子不一定有SHUFFLE {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("分区测试")
    conf.set("spark.default.parallelism", "500")
    val sc = new SparkContext(conf)


    val rdd1 = sc.makeRDD(1 to 10000)      //没有分区器，分区数=500
    val rdd2 = sc.makeRDD(1000 to 12000)   //没有分区器，分区数=500


    val rd11 = rdd1.map((_, 1))  // 没有分区器，沿用rdd1的分区数
      .groupBy(tp => tp._1)      // 上游没有分区器，所以用了HashPartitioner(默认并行度参数值；如果没有设，就是上游的最大分区数)

    val rd21 = rdd2.map((_, 1))  // 没有分区器，沿用rdd2的分区数
      .groupBy(tp => tp._1)     // 上游没有分区器，所以用了HashPartitioner(默认并行度参数值；如果没有设，就是上游的最大分区数)

    println(rd11.partitioner)
    println(rd11.partitions.size)

    println("======================")
    println(rd21.partitioner)
    println(rd21.partitions.size)

    // join没有引起shuffle
    //rd11.join(rd21).count()

    rd11.map(x=>x).join(rd21).count()

    // 让map映射后的rdd拥有前一个rdd的分区器
    rd11.mapPartitions(iter=>iter,true).join(rd21).count()

    // cogroup没有引起shuffle
    rd11.cogroup(rd21).count()
    // reduceByKey也没有引起shuffle
    rd11.reduceByKey((iter1,iter2)=>iter1).count()


    Thread.sleep(Long.MaxValue)
    sc.stop()
  }

}
