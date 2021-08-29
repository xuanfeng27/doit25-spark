package cn.doitedu.spark.demos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}

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
    val tmp1: RDD[(String, String, String, String)] = rdd_bt.map(s => (s.split(",")(0), s.split(",")(1), s.split(",")(2), s.split(",")(3)))
    val tmp2: RDD[(String, String, String, String, String, String)] = tmp1.mapPartitions(iter => {

      // 先不去迭代数据，而是建个mysql的连接
      val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/abc", "root", "123456")
      val stmt = conn.prepareStatement("select phone,age from battel_info where id = ?")

      // 开始迭代数据, 这种写法将无法关闭数据库连接
      val resIter: Iterator[(String, String, String, String, String, String)] = iter.flatMap(tp => {
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

        // 关闭外部资源的连接
        if (!iter.hasNext) {
          stmt.close()
          conn.close()
        }

        listBuffer.toList
      })



      // 返回一个迭代器
      resIter

      /*// 正确打开方式：返回一个自定义的迭代器
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
*/
    })


    // 5. mapPartitionsWithIndex ：  比 mapPartitions多得到了一个分区号
    val lst = List(1, 2, 3, 4, 5, 6, 7)
    val rdd8: RDD[Int] = sc.makeRDD(lst, 2)
    val rdd9 = rdd8.mapPartitionsWithIndex((partitionIndex, iter) => {
      iter.map(partitionIndex + " ->  " + _ * 10)
    })
    //rdd9.foreach(println)


    // 6. sample  抽样transformation算子  : wiReplcement 是否允许对相同元素重复采样
    val rdd10: RDD[Int] = rdd8.sample(false, 0.5)
    //rdd10.foreach(println)

    // action算子  : wiReplcement 是否允许对相同元素重复采样
    //val sampleInts: Array[Int] = rdd8.takeSample(false, 10)
    // println(sampleInts.mkString(","))

    // 7. union 算子  ： 相当于sql中的 union all
    val rdd_u1: RDD[(Int, String)] = sc.makeRDD(Seq((1, "a"), (2, "b"), (1, "a")))
    val rdd_u2: RDD[(Int, String)] = sc.makeRDD(Seq((3, "c"), (2, "d"), (1, "a")))
    val rdd_u: RDD[(Int, String)] = rdd_u1.union(rdd_u2)
    //rdd_u.foreach(println)


    // 8.intersection  ： 求交集，并会对结果去重
    val rdd_sec1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5))
    val rdd_sec2: RDD[Int] = sc.makeRDD(List(2, 3, 4, 6, 8, 2))
    val rdd_sec: RDD[Int] = rdd_sec1.intersection(rdd_sec2)
    //rdd_sec.foreach(println)


    // 9.distinct  ： 去重
    val rdd_dist: RDD[Int] = sc.makeRDD(List(1, 1, 3, 5, 2, 3, 4, 5))
    val rdd_dist_res = rdd_dist.distinct(3)

    // 10. groupByKey
    val rdd_bykey: RDD[(Int, String)] = sc.makeRDD(Seq((1, "a"), (2, "b"), (1, "a")))
    val groupby_res1: RDD[(Int, Iterable[(Int, String)])] = rdd_bykey.groupBy(tp => tp._1)
    val groupby_res2: RDD[(Int, Iterable[String])] = rdd_bykey.groupByKey()

    //groupby_res1.foreach(println)
    println("=================")
    //groupby_res2.foreach(println)


    // 11.  reduceByKey : 把key相同的数据组进行value聚合 ， 元素类型与聚合值类型必须一致
    val rdd_rdbk = sc.makeRDD(Seq(("a", 1), ("b", 2), ("a", 10)))
    val rdd_rdbk_res = rdd_rdbk.reduceByKey(_ + _)
    //rdd_rdbk_res.foreach(println)


    // 12: aggregateByKey :  元素类型与聚合值类型可以不一致
    val rdd_agbk = sc.makeRDD(Seq(("a", "1"), ("b", "2"), ("a", "10"), ("b", "20")))
    //rdd_agbk.aggregateByKey(0)((u,e)=>{ u+e.toInt},(u1,u2)=>{ u1+u2  })
    rdd_agbk.aggregateByKey(0)(_ + _.toInt, _ + _)


    // 13.sortByKey :  利用 rangePartitioner 进行 shuffle ，以便得到一个全局有序的结果
    val rdd_stbk = sc.makeRDD(
      Seq(("a", 1), ("b", 2), ("a", 10), ("b", 20), ("c", 40), ("d", 40), ("d", 18), ("d", 30), ("e", 10)),
      3
    )

    // 按k全局有序
    val rdd_stbk_res = rdd_stbk.sortByKey(true, 2)
    //rdd_stbk_res.saveAsTextFile("dataout/stbk/")

    // 按kv全局有序 ， 把原来数据的 kv,映射成(kv,null) ，就能利用sortByKey做全局排序
    val rdd_stbk_res2 = rdd_stbk.map(tp => (tp, null)).sortByKey(true, 2)
    //rdd_stbk_res2.saveAsTextFile("dataout/stbk2/")

    // TODO  练习题 用sortByKey算子对 rdd_exc 做全局排序，而且要求最后的结果的分区数量为 2
    val rdd_exc = sc.makeRDD(Seq(
      new Person("张嘉俊", 23, "广东"),
      new Person("王兴国", 24, "广东"),
      new Person("郑格非", 22, "福建"),
      new Person("郝瑞瑞", 21, "福建"),
      new Person("柳坤坤", 25, "辽宁"),
      new Person("李宗文", 32, "辽宁")
    ))

    implicit val ord = new Ordering[Person] {
      override def compare(x: Person, y: Person): Int = x.age.compare(y.age)
    }
    val value: RDD[(Person, Null)] = rdd_exc.map(p => (p, null))

    val sorted: RDD[(Person, Null)] = value.sortByKey(true,4)

    /* val ordering = implicitly[Ordering[Person]]
    val part = new RangePartitioner(4, value, true)
    val sorted_custom = new ShuffledRDD[Person, Null, Null](value, part)
      .setKeyOrdering(if (true) ordering else ordering.reverse) */


    // 14.join 算子 ：   类似于 sql中  inner  join
    val rdd_join1 = sc.makeRDD(Seq(("a", 1), ("a", 10), ("b", 2), ("e", 10)))
    val rdd_join2 = sc.makeRDD(Seq(("a", 10), ("b", 20), ("c", 10)))
    println("======内连接===========")
    val rdd_join_res1: RDD[(String, (Int, Int))] = rdd_join1.join(rdd_join2)
    //rdd_join_res1.foreach(println)
    println("======左外连接===========")
    val rdd_join_res2: RDD[(String, (Int, Option[Int]))] = rdd_join1.leftOuterJoin(rdd_join2)
    //rdd_join_res2.foreach(println)
    println("======全外连接===========")
    val rdd_join_res3: RDD[(String, (Option[Int], Option[Int]))] = rdd_join1.fullOuterJoin(rdd_join2)


    // 15. cogroup 算子  ：协同分组
    val rdd_cg1 = sc.makeRDD(Seq(("a", 1), ("a", 10), ("b", 2), ("e", 10)))
    val rdd_cg2 = sc.makeRDD(Seq(("a", "10"), ("a", "80"), ("a", "60"), ("b", "20"), ("c", "20")))
    val rdd_cg_res: RDD[(String, (Iterable[Int], Iterable[String]))] = rdd_cg1.cogroup(rdd_cg2)
    // (a,(CompactBuffer(1, 10),CompactBuffer(10, 80, 60)))
    //rdd_cg_res.foreach(println)
    // TODO 利用 cogroup 算子 实现上述两个rdd的： 内join效果， 外join效果

    // 内join效果
    // a,1,10
    // a,1,80
    // a,1,60
    // a,10,10
    // a,10,80
    // a,10,60
    val filtered1 = rdd_cg_res.filter(tp=> !tp._2._1.isEmpty & !tp._2._2.isEmpty )

    val joined = filtered1.flatMap(tp=>{
      val iter1: Iterable[Int] = tp._2._1
      val iter2: Iterable[String] = tp._2._2
      for( e1 <- iter1 ; e2 <- iter2) yield {
        (tp._1,e1,e2)
      }
    })


    // 左外join效果
    val filtered2 = rdd_cg_res.filter(tp=> !tp._2._1.isEmpty )
    val joinedLeft = filtered1.flatMap(tp=>{
      val iter1: Iterable[Int] = tp._2._1
      val iter2: Iterable[String] = tp._2._2
      // ("e",(Buff[10,80],Empty))
      if(iter2.isEmpty){
        iter1.map(e1=>(tp._1,e1,null))
      }else{
        for( e1 <- iter1 ; e2 <- iter2) yield {
          (tp._1,e1,e2)
        }
      }
    })



    // 16. cartesian 算子  ： 求两个rdd的笛卡尔积
    val rdd_carte1 = sc.makeRDD(Seq(("a", 1), ("b", 2)))
    val rdd_carte2 = sc.makeRDD(Seq(("a", "10"), ("b", "12"), ("c", "20")))
    val rdd_carte_res: RDD[((String, Int), (String, String))] = rdd_carte1.cartesian(rdd_carte2)
    rdd_carte_res.foreach(println)

  }
}

class Person(val name: String, val age: Int, val province: String)