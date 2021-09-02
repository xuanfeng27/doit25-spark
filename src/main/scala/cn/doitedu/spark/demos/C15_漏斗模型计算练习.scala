package cn.doitedu.spark.demos

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object C15_漏斗模型计算练习 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("漏斗模型计算").setMaster("local")
    val sc = new SparkContext(conf)

    // 加载日志文件
    val logRDD: RDD[String] = sc.textFile("data/app_log_2021-06-07.log")

    // 事件过滤
    val filtered: RDD[(String, String, Long)] = logRDD.map(line => {
      val jSONObject: JSONObject = JSON.parseObject(line)
      val deviceId: String = jSONObject.getString("deviceId")
      val eventId: String = jSONObject.getString("eventId")
      val timeStamp: Long = jSONObject.getLong("timeStamp")

      val properties: JSONObject = jSONObject.getJSONObject("properties")

      var flag = eventId match {
        case "search" if (properties.getString("keywords").contains("QRN")) => true
        case "addCart" if (properties.getString("productId").startsWith("9")) => true
        case "submitOrder" => true
        case _ => false
      }

      if (flag) (deviceId, eventId, timeStamp) else ("", "", -1L)
    }).filter(tp => StringUtils.isNotBlank(tp._1))


    // 按用户分组，收集他的行为事件并拼接成字符串
    val actStr: RDD[(String, String)] = filtered.groupBy(_._1).mapValues(iter => {
      // 对一个人的行为按时间戳排序
      val lst: List[(String, String, Long)] = iter.toList
      lst
        .sortBy(tp => tp._3)
        // 取出每一条数据中的事件id
        .map(_._2)
        // 拼接字符串
        .mkString(",")
    })


    // 匹配正则，输出结果
    val maxStepRDD: RDD[(String, Int)] = actStr.map(tp=>{
      val deviceId  = tp._1
      val str  = tp._2

      val maxStep = str match {
        case s:String  if(s.matches(".*?search.*?addCart.*?submitOrder.*?")) => 3
        case s:String  if(s.matches(".*?search.*?addCart.*?")) => 2
        case s:String  if(s.matches(".*?search.*?")) => 1
        case _ => 0
      }
      (deviceId,maxStep)
    }).filter(tp=>tp._2>0)

    // 注意，上面的结果，需要做一个处理：
    // d01,3  =>
    //      d01,1
    //      d01,2
    //      d01,3
    val stepRDD = maxStepRDD.flatMap(tp=>{
      for( i <- 1 to tp._2)  yield{
        (tp._1,i)
      }
    })


    // 统计结果： 每一步完成的人数
    val report = stepRDD.map(tp=>(tp._2,tp._1)).groupByKey().mapValues(iter=>iter.size)

    report.foreach(println)

    sc.stop()
  }
}
