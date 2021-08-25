package cn.doitedu.spark.demos

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.DriverManager
import scala.collection.mutable.ListBuffer

object DemoMapPartitions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName(DemoMapPartitions.getClass.getName)
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.textFile("data/battel.txt")

    val result = rdd.map(str=>{
      val arr = str.split(",")
      (arr(0),arr(1),arr(2),arr(3))
    }).mapPartitions(iter=>{
      val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/day01", "root", "daydayup")
      val stat = conn.prepareStatement("select phone,age from battel_info where id = ?")

      val res = iter.flatMap(tp=>{
        val buffer = ListBuffer[(String,String,String,String,String,String)]()
        val id = tp._1.toInt
        stat.setInt(1,id)
        val rs = stat.executeQuery()
        while (rs.next()){
          val phone = rs.getString("phone")
          val age = rs.getString("age")
          buffer+=((tp._1,tp._2,tp._3,tp._4,phone,age))
        }
        rs.close()

        if(!iter.hasNext) {
          stat.close()
          conn.close()
        }
        buffer.toList
      })



      res
    })


    result.foreach(println)





  }
}
