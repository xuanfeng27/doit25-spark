package cn.doitedu.spark.mydemos

import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MyTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(MyTest.getClass.getName)
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.textFile("data/app_log_2021-06-07.log")
    val rdd1: RDD[(String, String)] = rdd
      .map(str=>{
      val jSONObject = JSON.parseObject(str)
      val deviceId = jSONObject.getString("deviceId")
      deviceId
    }).distinct(3)
      .map(id=>(id,id))

   //
    val rdd2: RDD[(String, String, String)] = sc.emptyRDD[String]
      .map(u=>{
        val arr = u.split(",")
        (arr(0),arr(1),arr(2))
      })
    //闭合的
    val rdd3: RDD[(String, String, String)] = rdd2.filter(!_._3.equals("9999-12-31"))
    //未闭合
    val rdd4: RDD[(String, (String, String))] = rdd2.filter(_._3.equals("9999-12-31")).map(tp=>(tp._1,(tp._2,tp._3)))
    val res1: RDD[(String, (Option[String], Option[(String, String)]))] = rdd1.fullOuterJoin(rdd4)

    val res2: RDD[Option[(String, String, String)]] = res1.map(tp=>{
      tp._2 match {
        case (Some(deviceId), None) => Some((deviceId, "2021-06-07", "9999-12-31"))
        case (Some(deviceId), Some((startDate, endDate))) => Some((deviceId, startDate, endDate))
        case (None, Some((startDate, endDate))) => Some((tp._1, startDate, "2021-06-07"))
        case _ => None
      }
    })

    val res: RDD[String] =
      res2
        .filter(_.isDefined)
        .map(_.get)
        .union(rdd3)
        .map(tp=>tp._1+","+tp._2+","+tp._3)

    res.saveAsTextFile("dataout/myTest/")


    sc.stop()
  }
}
