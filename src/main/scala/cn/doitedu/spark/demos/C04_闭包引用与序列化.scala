package cn.doitedu.spark.demos

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

import scala.collection.mutable

object C04_闭包与广播变量 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("闭包与广播")
    conf.set("spark.default.parallelism", "2")
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 8), ("b", 6), ("c", 4)))


    var count: Int = 0 // 此处定义的count对象，在Driver端的jvm中

    val rdd1 = rdd.map(tp => {
      count += 1 // 此处的 count是经过闭包处理后（序列化后）再反序列化得到的对象，在executor端的 Task对象中
      (tp._1.toUpperCase, tp._2 * 10)
    }).collect()

    println(count)


    val hashMap = new mutable.HashMap[String, Int]() // 此处定义的hashmap对象，在Driver端的jvm中

    rdd.map(tp => {
      hashMap += tp // 此处的 hashmap是经过闭包处理后（序列化后）再反序列化得到的对象，在executor端的 Task对象中
      val id: Int = TaskContext.getPartitionId()
      println(id + ": " + hashMap)
    }).count()
    println(hashMap)


    /**
     * 自定义类型，如果没有实现序列化，则很容易在executor端执行时报序列化异常
     * 虽然下面示例代码中，一种报序列化异常，一种不报
     * 但是，为了避免出现问题，建议一律将自定义类型实现序列化接口
     */
    class Phone(var brand: String, var price: Double)
    val p = new Phone("xx", 8.8)
    val res = rdd.map(tp => {
      p.brand = tp._1 // 这里就是一个闭包引用，每个task都会拥有一个p对象的序列化副本,这里phone对象没有实现序列化，直接task生成就失败
      p.price = tp._2.toDouble
    })

    val resRdd = rdd.map(tp => {
      new Phone(tp._1, tp._2) // 这里并没有引用外部的对象，所以不存在f序列化检查失败的问题，所以可以运行起来
    })
      //.map(phone => (phone.brand, phone.price))
      //.reduceByKey(_ + _) // 这里有shuffle，但是shuffle写出的是 2元组，它能序列化，所以不会报错
      .groupBy(p => p.brand) // 这里有shuffle，而且shuffle写出phone对象，它不能序列化，所以报错
      .mapValues(_.size)

    resRdd.foreach(println)

    sc.stop()

    val r = sc.makeRDD(List("a", "a", "b", "c", "a", "d", "c"), 2)
    r.map((_, 1)).reduceByKey(_ + _).map(tp => (tp._1, tp._2 * 10)).foreach(println)

  }
}
