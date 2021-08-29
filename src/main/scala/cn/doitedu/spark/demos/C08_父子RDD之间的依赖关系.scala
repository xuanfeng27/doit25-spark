package cn.doitedu.spark.demos

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object C08_父子RDD之间的依赖关系 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("父子RDD之间的依赖关系")
    val sc = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.makeRDD(1 to 10000, 4)

    val res = rdd1.map(x=>(x,10)).reduceByKey( _ + _,2 ).map(tp=>(tp._1,tp._2 *30))

    res.count()

    Thread.sleep(Long.MaxValue)

    sc.stop()
  }
}
