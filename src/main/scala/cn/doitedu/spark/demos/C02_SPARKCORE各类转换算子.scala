package cn.doitedu.spark.demos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import scala.collection.{AbstractIterator, Iterator}
import scala.collection.Iterator.empty
import scala.collection.mutable.ListBuffer

object C02_SPARKCORE各类转换算子 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)


    val conf = new SparkConf
    conf.setMaster("local")
    conf.setAppName("各类算子测试")

    val sc = new SparkContext(conf)

    // 读数据
    val rdd_bt: RDD[String] = sc.textFile("data/battel.txt")
    val rdd_wc: RDD[String] = sc.textFile("data/wordcount.txt")

    /*
     * 算子测试
     */

    // 1. map算子 ；  传入函数： T => U   最终得到  RDD[U]
    val res1 = rdd_bt.map(s => {
      val arr: Array[String] = s.split(",")
      (arr(0), arr(1), arr(2), arr(3))
    })
    // res1.foreach(println)

    // 2. filter算子
    val res2 = rdd_bt
      .map(s => {
        val arr: Array[String] = s.split(",")
        (arr(0), arr(1), arr(2), arr(3))
      })
      .filter(tp => tp._4.toInt >= 400)
    // res2.foreach(println)


    // 3. flatMap 算子；   传入函数： T => List[U]  最终得到  RDD[U]
    val res3: RDD[String] = rdd_wc.flatMap(s => s.split("\\s+"))
    // res3.foreach(println)


    // 4. mapPartitions
    // 一个分区调用一次你传的函数, task程序会把它负责的分区的所有数据的一个迭代器传给你的函数
    // 相对于map来说，mappartitions通常用于数据处理过程中需要访问外部资源的场景
    val res4: RDD[String] = rdd_wc.mapPartitions(iter => {

      println("=====外层函数，一个分区的task，只会调用一次======")

      val iter2: Iterator[String] = iter.flatMap(s => {
        println("====内层函数，每一行都会被调用====")
        s.split("\\s+")
      })

      iter2
    })
    // res4.foreach(println)

    // mapPartitions 真实应用场景示例
    // 从battel.txt中加载数据，并从mysql中查询到每一个人的手机号，年龄信息，拼接到一起输出
    val res5: RDD[(String, String, String, String, String, String)] = rdd_bt
      .map(s => (s.split(",")(0), s.split(",")(1), s.split(",")(2), s.split(",")(3)))
      .mapPartitions(iter => {

        // 先不去迭代数据，而是建个mysql的连接
        val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/abc", "root", "123456")
        val stmt = conn.prepareStatement("select phone,age from battel_info where id = ?")

        // 开始迭代数据, 这种写法将无法关闭数据库连接
        /*val resIter: Iterator[(String, String, String, String, String, String)] = iter.flatMap(tp => {
          println("使用连接........")
          val id = tp._1.toInt
          stmt.setInt(1, id)
          val rs: ResultSet = stmt.executeQuery() // 迭代器的思想无处不在

          val listBuffer = new ListBuffer[(String, String, String, String, String, String)]
          while (rs.next()) {
            val phone: String = rs.getString("phone")
            val age: String = rs.getString("age")
            listBuffer += ((tp._1, tp._2, tp._3, tp._4, phone, age))
          }
          rs.close()
          listBuffer.toList
        })

        // 关闭外部资源的连接
        println("关闭连接........")
        stmt.close()
        conn.close()

        // 返回一个迭代器
        resIter
        */

        // 正确打开方式：返回一个自定义的迭代器
        new AbstractIterator[(String, String, String, String, String, String)] {

          // 内层迭代器
          var cur: Iterator[(String, String, String, String, String, String)] = Iterator.empty

          override def hasNext: Boolean = {
            // 如果内层迭代器还有数据，则返回true
            if (cur.hasNext) {
              true
            }
            // 如果内层迭代器已经没有数据了，则看外层迭代器是否还有数据
            else if (iter.hasNext) {
              // 从外层迭代器获取一条记录，处理成多条记录的结果转成迭代器后赋给 内存迭代器
              val tp = iter.next()
              val id = tp._1.toInt
              stmt.setInt(1, id)
              val rs: ResultSet = stmt.executeQuery() // 迭代器的思想无处不在

              val listBuffer = new ListBuffer[(String, String, String, String, String, String)]
              while (rs.next()) {
                val phone: String = rs.getString("phone")
                val age: String = rs.getString("age")
                listBuffer += ((tp._1, tp._2, tp._3, tp._4, phone, age))
              }
              rs.close()

              cur = listBuffer.toIterator
              // 返回新的内层迭代器的hasNext结果
              cur.hasNext
            }
            // 如果外、内两层迭代器都已经没有数据，则准备返回false，此时可以关闭数据库连接了
            else {
              stmt.close()
              conn.close()
              false
            }
          }

          override def next(): (String, String, String, String, String, String) = {
            (if(cur.hasNext) cur else Iterator.empty).next()
          }
        }

      })

    res5.foreach(println)

    // 输出


  }
}
