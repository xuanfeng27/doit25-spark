package cn.doitedu.spark.mydemos

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
//步骤1：  完成行为  search，且行为属性中有 ： keyword = 咖啡
// 步骤2：  完成行为  addCart，且行为属性中有： productId以101开头
//步骤3：  完成行为  submitOrder
// 完成每一个步骤的人数

object LogAnalysis {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("loudou").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.textFile("data/app_log_2021-06-07.log")
    val rdd1: RDD[(String, String, Long)] = rdd.map(str => {
      val jb = JSON.parseObject(str)
      val deviceId: String = jb.getString("deviceId")
      val eventId: String = jb.getString("eventId")
      val timeStamp: Long = jb.getLong("timeStamp")
      val props: JSONObject = jb.getJSONObject("properties")
      eventId match {
        case "search" if (props.getString("keywords").contains("YLoo")) => (deviceId, eventId, timeStamp)
        case "addCart" if (props.getString("productId").startsWith("9")) => (deviceId, eventId, timeStamp)
        case "submitOrder" => (deviceId, eventId, timeStamp)
        case _ => ("", "", -1L)
      }
    }).filter(tp=>StringUtils.isNotBlank(tp._1))

    val rdd2: RDD[(String, String)] = rdd1.groupBy(_._1).mapValues(tp => {
      tp.toList
        .sortBy(_._3)
        .map(_._2)
        .mkString(",")
    })

    val rdd3: RDD[(String, Int)] = rdd2.map(tp => {
      tp._2 match {
        case str: String if str.matches(".*?search.*?addCart.*?submitOrder.*?") => (tp._1, 3)
        case str: String if str.matches(".*?search.*?addCart.*?") => (tp._1, 2)
        case str: String if str.matches(".*?search.*?") => (tp._1, 1)
        case _ => ("", 0)
      }
    }).filter(_._2 > 0)

    //处理
    val rdd4: RDD[(String, String)] = rdd3.flatMap(tp => {
      for (i <- 1 to tp._2) yield {
        (tp._1, "步骤"+i)
      }
    })

    val res: RDD[(String, Int)] = rdd4.groupBy(_._2).mapValues(_.size)

    res.foreach(println)



    sc.stop()
  }
}
