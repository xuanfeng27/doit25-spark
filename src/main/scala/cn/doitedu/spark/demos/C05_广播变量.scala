package cn.doitedu.spark.demos

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{BufferedReader, FileReader}
import scala.collection.mutable
import scala.io.{BufferedSource, Source}

object C05_广播变量 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("关联")
    val sc = new SparkContext(conf)

    // 1. 从文件中创建 rdd
    val log: RDD[String] = sc.textFile("data/action.log")
    val user: RDD[String] = sc.textFile("data/user.txt")


    // 2. 把数据先变成 KV 结构
    val kvLog = log.map(s=>{
      val arr: Array[String] = s.split(",")
      (arr(0),s)
    })

    val kvUser = user.map(s=>{
      val arr: Array[String] = s.split(",")
      val userInfo = arr(1) + "," + arr(2)
      (arr(0), userInfo)
    })

    // 3.调用rdd 的 join算子  : 本质上是一种shuffle join （reduce端join）
   /* val joined: RDD[(String, (String, String))] = kvLog.join(kvUser)
    joined.map(tp=> tp._2._1 + "," + tp._2._2).foreach(println)*/


    /**
     * 用map端join来实现（没有shuffle过程）
     * 1. cache file
     */
    /*sc.addFile("data/user.txt")   // 这个文件会进入每一个task所在的容器
    val joined2 = kvLog.mapPartitions(iter =>{
      // 加载task端的本地文件
      val source: BufferedSource = Source.fromFile("user.txt")
      val infoMap: Map[String, String] = source.getLines().map(s => {
        val arr: Array[String] = s.split(",")
        (arr(0), arr(1) + "," + arr(2))
      }).toMap

      // 开始迭代日志数据
      iter.map(tp=>{
        tp._2 +","+ infoMap.get(tp._1).get
      })

      source.close()
      iter
    })*/

    /**
     * 用map端join来实现（没有shuffle过程）
     * 2. 将小表直接闭包到task的函数中去
     */
    /*
    val source: BufferedSource = Source.fromFile("data/user.txt")
    val infoMap: Map[String, String] = source.getLines().map(s => {
      val arr: Array[String] = s.split(",")
      (arr(0), arr(1) + "," + arr(2))
    }).toMap

    // 在rdd算子的函数中直接引用driver端的对象，会被序列化到task中
    // 将来各个task反序列化后，就是每个task都持有了一个hashmap
    val joined2 = kvLog.map(tp=>{
      tp._2 + "," + infoMap.get(tp._1).get
    })
    */


    /**
     * 用map端join来实现（没有shuffle过程）
     * 3. 将小表以广播变量的形式发送到每一个Executor进程中，一个Executor只一份hashmap对象
     * 在同一个Executor中运行的task可以共享这一份hashmap对象
     *
     * 重中之重：  广播变量的方式  ，和 task闭包引用的方式 ，究竟有何区别！！！
     *
     */
    val source: BufferedSource = Source.fromFile("data/user.txt")
    val infoMap: Map[String, String] = source.getLines().map(s => {
      val arr: Array[String] = s.split(",")
      (arr(0), arr(1) + "," + arr(2))
    }).toMap

    // 将driver的数据对象广播出去
    val bc: Broadcast[Map[String, String]] = sc.broadcast(infoMap)

    val joined3 = kvLog.map(tp=>{
      // 在算子函数中，可以通过广播变量的句柄获取到广播变量的对象
      val infoMap2: Map[String, String] = bc.value
      tp._2 + "," + infoMap2.get(tp._1).get
    })

    joined3.foreach(println)

    sc.stop()
  }

}
