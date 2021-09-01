package cn.doitedu.spark.demos

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object C13_RDD缓存 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("分区测试")
      .set("spark.default.parallelism", "500")
    val sc = new SparkContext(conf)

    val rdd_o: RDD[(Order, Int)] = sc.makeRDD(Seq(
      (Order(1, "柳坤", 200, "团购订单"), 1),
      (Order(2, "兴国", 300, "普通订单"), 2),
      (Order(3, "柳坤", 400, "团购订单"), 3),
      (Order(4, "广磊", 500, "普通订单"), 4),
      (Order(5, "兴国", 600, "普通订单"), 5)
    ), 4)

    // 相同人相同订单类型的总金额
    val rdd1 = rdd_o.map(tp => ((tp._1.account, tp._1.orderType), tp._1.amt))
    val rdd2 = rdd1.reduceByKey(_ + _)  // 中间结果   如果后续流程中需要对它反复使用，则可以把它物化

    // spark中对rdd物化的手段，就是将它缓存起来
    rdd2.cache()  //  persist(StorageLevel.MEMORY_ONLY)
    rdd2.persist(StorageLevel.MEMORY_AND_DISK)  // 可以自己控制存储级别
    /**
        case "NONE" => NONE
        case "DISK_ONLY" => DISK_ONLY
        case "DISK_ONLY_2" => DISK_ONLY_2
        case "MEMORY_ONLY" => MEMORY_ONLY
        case "MEMORY_ONLY_2" => MEMORY_ONLY_2
        case "MEMORY_ONLY_SER" => MEMORY_ONLY_SER
        case "MEMORY_ONLY_SER_2" => MEMORY_ONLY_SER_2
        case "MEMORY_AND_DISK" => MEMORY_AND_DISK
        case "MEMORY_AND_DISK_2" => MEMORY_AND_DISK_2
        case "MEMORY_AND_DISK_SER" => MEMORY_AND_DISK_SER
        case "MEMORY_AND_DISK_SER_2" => MEMORY_AND_DISK_SER_2
        case "OFF_HEAP" => OFF_HEAP
    */


    // 在上面的结果res中，求TOP3
    val rdd3 = rdd2.map(tp => (-tp._2, tp._1))
    val res1 = rdd3.takeOrdered(3).map(tp => (tp._2, tp._1))

    // 在上面的结果res中，求每个人的订单总金额
    val rdd4 = rdd2.map(tp => (tp._1._1, tp._2))
    val rdd5 = rdd4.reduceByKey(_ + _)
    val res2 = rdd5.collect()

    sc.stop()
  }
}
