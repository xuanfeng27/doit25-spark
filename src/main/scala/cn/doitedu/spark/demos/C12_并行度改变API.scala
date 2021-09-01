package cn.doitedu.spark.demos

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object C12_并行度改变API {
  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("分区测试")
      .set("spark.default.parallelism", "500")
    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD((1 to 10000).zip(10000 to 20000), 100)
    println(rdd1.partitions.size)


    // coalesce
    // 修改并行度：可以改大，也可以改小； 取决于算子中的参数： shuffle
    // 如果shuffle==true，则该算子会生成一个ShuffledRDD，产生了shuffle
    // 如果shuffle==false,则概算自不会产生shuffle，但是分区数只能该小；（改大不生效，自动等于前一个rdd的并行度）
    val rdd4 = rdd1.coalesce(20, true) // 20
    val rdd5 = rdd1.coalesce(200, false) // 100
    val rdd6 = rdd1.coalesce(200, true) // 200
    println(rdd4.partitions.size)
    println(rdd5.partitions.size)
    println(rdd6.partitions.size)


    // repartition(200)
    // 调用的就是coalesce(shuffle=true,200)
    // 所以，repartition可以把并行度改大也可以改小，而且一定会产生shuffle
    val rdd2: RDD[(Int, Int)] = rdd1.repartition(10) // 10
    val rdd3: RDD[(Int, Int)] = rdd1.repartition(200) // 200
    println(rdd2.partitions.size)
    println(rdd3.partitions.size)


    // partitionBy
    // 允许开发人员传入一个Partitioner来对上游数据进行重新分区
    // 自己传入的Partitioner就可以控制根据Key中的哪个成分来分区
    val rdd_o = sc.makeRDD(Seq(
      (Order(1, "柳坤1", 200, "团购订单"),1),
      (Order(2, "柳坤2", 300, "普通订单"),2),
      (Order(3, "柳坤3", 400, "团购订单"),3),
      (Order(4, "柳坤4", 500, "普通订单"),4),
      (Order(5, "柳坤5", 600, "普通订单"),5)
    ), 4)


    // 根据订单的订单类型来分区
    val rdd_v: RDD[(Order, Int)] = rdd_o.partitionBy(new OrderPartitioner(2))
    println(rdd_v.partitioner)
    println(rdd_v.partitions.size)


    sc.stop()
  }
}

case class Order(id: Int, account: String, amt: Double, orderType: String)

class OrderPartitioner(numPartitions: Int) extends HashPartitioner(numPartitions:Int) {
  override def getPartition(key: Any): Int = {
    val order: Order = key.asInstanceOf[Order]
    super.getPartition(order.orderType)
  }
}