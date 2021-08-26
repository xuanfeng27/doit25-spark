package cn.doitedu.spark.demos

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object C03_SPARKCORE各类行动算子 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("行动算子测试")
    val sc = new SparkContext(conf)


    val rdd= sc.makeRDD(Seq(("a",10 ), ("a",80 ),("a",60 ),("b",20), ("c",20)))

    // reduce ： 把整个rdd根据你的逻辑聚合成1个值
    val v:Int = rdd.map(_._2)reduce(_+_)
    println(v)

    // collect
    val arr: Array[(String, Int)] = rdd.collect()
    println(arr.mkString(","))


    // count
    val cnt: Long = rdd.count()
    println(cnt)


    // first
    val tuple: (String, Int) = rdd.first()
    println(tuple)


    // take(n)
    val tuples: Array[(String, Int)] = rdd.take(3)
    println(tuples.mkString(" , "))


    // takeOrdered(n,[ordering])
    val rdd2: RDD[(String, Int)] = sc.makeRDD(Seq(("zs", 18), ("ls", 19), ("zs", 28), ("ls", 21)))
    implicit val ord = Ordering[(String,Int)].on[(String,Int)](tp=>(tp._1, - tp._2))
    val res: Array[(String, Int)] = rdd2.takeOrdered(3)(ord)
    println(res.mkString(" , "))


    // saveAsTextFile   : 把rdd的数据以字符串的形式写入 文本文件
    //rdd2.saveAsTextFile("dataout/savetext/")

    // saveAsSeqeueceFile   : 把rdd的数据以字符串的形式写入 文本文件
    //rdd2.saveAsSequenceFile("dataout/saveseq/")

    val res2: collection.Map[String, Long] = rdd2.countByKey()
    println(res2)







  }
}
