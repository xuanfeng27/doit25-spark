package cn.doitedu.spark.mydemos

import org.apache.spark.{SparkConf, SparkContext}

object WordCnt {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(WordCnt.getClass.getName).setMaster("local")
    val sc = new SparkContext(conf)
    val res = sc.textFile("data/wordcount.txt")   //"hdfs://doit:8020/sparktest/WordCount/input"
      .flatMap(_.split("\\s+"))
      .map(word=>(word,1))
      .reduceByKey((x,y)=>x+y)

    res.saveAsTextFile("dataout/res")    //"hdfs://doit:8020/sparktest/WordCount/output"

    sc.stop()
  }
}
