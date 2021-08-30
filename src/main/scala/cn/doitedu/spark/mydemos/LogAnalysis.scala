package cn.doitedu.spark.mydemos

import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
//步骤1：  完成行为  search，且行为属性中有 ： keyword = 咖啡
// 步骤2：  完成行为  addCart，且行为属性中有： productId以101开头
//步骤3：  完成行为  submitOrder
// 完成每一个步骤的人数

object LogAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("log")
    val sc = new SparkContext(conf)
    //val rdd = sc.textFile("D:\\zll\\doitedu\\doitNotes\\doit25-spark\\doit25-spark-day03\\applog\\applog\\")
    val rdd = sc.textFile("data/testlog.log")
    val rdd1: RDD[(String, String, String,String)] = rdd.map(json=>{
      val jSONObject = JSON.parseObject(json)
      val deviceId = jSONObject.getString("deviceId")
      val eventId = jSONObject.getString("eventId")
      val timeStamp = jSONObject.getString("timeStamp")
      val properties = jSONObject.getString("properties")
      val prop = JSON.parseObject(properties)
      val keyword = prop.getString("keywords")
      val productId = prop.getString("productId")
      val props =keyword+productId
      (deviceId,eventId,props,timeStamp)
    })
    val rdd2: RDD[(String, Iterable[(String, String, String, String)])] = rdd1.groupBy(_._1)
    val rdd3: RDD[(String, (String, String))] = rdd2.mapValues(iter => {
      val list = iter.toList.sortBy(_._4)
      val eventStr = list.map(_._2).mkString
      val propStr = list.map(_._3).mkString
      (eventStr,propStr)
    })

    val rdd4: RDD[(String, String)] = rdd3.mapValues(tp => {
      tp match {
        case (a, b) if a.matches(".*search.*addCart.*submitOrder.*") && b.matches(".*咖啡101.*") => "123"
        case (a, b) if a.matches(".*search.*addCart.*") && b.matches(".*咖啡101.*") => "12"
        case (a, b) if a.matches(".*search.*") && b.matches(".*咖啡.*") => "1"
        case _ => ""
      }
    })

    val rdd5: RDD[(String, Int)] = rdd4.filter(!_._2.equals("")).groupBy(_._2).mapValues(_.size)
    rdd5.foreach(println)
  }
}
