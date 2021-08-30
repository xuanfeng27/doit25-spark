package cn.doitedu.spark.mydemos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object DemoTransformation {
  def main(args: Array[String]): Unit = {
    //不打印INFO级别日志打印
    Logger.getLogger("org").setLevel(Level.WARN)

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName(DemoTransformation.getClass.getName)
    val sc = new SparkContext(conf)
    val rdd_tf = sc.textFile("data/battel.txt")

    /**
     * 转换算子
     * map ,filter, flatMap, mapPartitions,  mapPartitionsWithIndex, sample,union
     */
    val list = List(1,2,3,4,5,6,7)
    val rdd_lst = sc.makeRDD(list, 2)
    rdd_lst.mapPartitionsWithIndex((idx, iter) => {
      iter.map(idx + "---" + _ * 10)
    })
    //  .foreach(println)

    //union 相当于sql中的 union all
    val rdd_s1 = sc.makeRDD(Seq((1, "a"), (2, "b"), (3, "c")))
    val rdd_s2 = sc.makeRDD(Seq((1, "aa"), (20, "b"), (3, "c")))
    val rdd_s3: RDD[(Int, String)] = rdd_s1.union(rdd_s2)
   // rdd_s3.foreach(println)


    //intersection 交集，去重
    //rdd_s1.intersection(rdd_s2).foreach(println)

    //distinct  ： 去重
   // sc.makeRDD(List(1,2,1,3,3,4)).distinct(2).foreach(println)


    //groupByKey
    val rdd_gk = sc.makeRDD(Seq((1, "a"), (1, "b"), (2, "c")))
   // rdd_gk.groupBy(_._1).foreach(println)
    val rdd_g: RDD[(Int, Iterable[String])] = rdd_gk.groupByKey()
   // rdd_g.foreach(println)//(1,CompactBuffer(a, b)) , (2,CompactBuffer(c))

      //reduceByKey 把key相同的数据组进行value聚合 ， 元素类型与聚合值类型必须一致
      rdd_gk.reduceByKey(_+_).foreach(println)

    //aggregateByKey :  元素类型与聚合值类型可以不一致
    val rdd_gg = sc.makeRDD(Seq((1, "11"), (1, "121"), (2, "222")))
    val rdd_gr = rdd_gg.aggregateByKey(0)((a, b) => a + b.toInt, (x, y) => x + y)
    //rdd_gr.foreach(println)

    //sortByKey
    val rdd_st = sc.makeRDD(
      Seq(("a",1 ), ("b",2), ("a",10), ("b",20), ("c",40), ("d",40),("d",18), ("d",30), ("e",10)),
      3
    )
    // 按k全局有序
   // rdd_st.sortByKey(true,2).saveAsTextFile("dataout/sortByKey/")
    //按kv全局有序
    //rdd_st.map((_,null)).sortByKey(true,2).saveAsTextFile("dataout/sortByKey2/")


    //用sortByKey算子对 rdd_exc 做全局排序，而且要求最后的结果的分区数量为 2
    val rdd_exc = sc.makeRDD(Seq(
      new Person("张嘉俊",23,"广东"),
      new Person("王兴国",24,"广东"),
      new Person("郑格非",22,"福建"),
      new Person("郝瑞瑞",21,"福建"),
      new Person("柳坤坤",25,"辽宁"),
      new Person("李宗文",32,"辽宁")
    ))

    implicit val ord: Ordering[Person] = new Ordering[Person] {
      def compare(x: Person, y: Person): Int = {
          x.age.compare(y.age)
      }
    }

     rdd_exc.
       map(p => (p, null))
       .sortByKey(true,2)
       .saveAsTextFile("dataout/sortPerson/")





    // cogroup 算子  ：协同分组
    val rdd_cg1 = sc.makeRDD(Seq(("a",1 ), ("a",10), ("b",2), ("e",10)))
    val rdd_cg2 = sc.makeRDD(Seq(("a","10" ), ("a","80" ),("a","60" ),("b","20"), ("c","20")))

    val joind: RDD[(String, (Int, String))] = rdd_cg1.join(rdd_cg2)

    val rdd_cg_res: RDD[(String, (Iterable[Int], Iterable[String]))] = rdd_cg1.cogroup(rdd_cg2)
    //    (a,(CompactBuffer(1, 10),CompactBuffer(10, 80, 60)))
    //    rdd_cg_res.foreach(println)
    // 利用 cogroup 算子 实现上述两个rdd的： 内join效果， 外join效果
    rdd_cg_res
        .filter(tp=> tp._2._1.nonEmpty && tp._2._2.nonEmpty )
        .flatMap(tp=>{
          val key = tp._1
          val iter1 = tp._2._1
          val iter2 = tp._2._2
          for (e1 <- iter1 ; e2<-iter2) yield{
            (key,(e1,e2))
          }
        }).foreach(println)



  }
}

case class Person(val name:String,val age:Int,val province:String) extends Serializable