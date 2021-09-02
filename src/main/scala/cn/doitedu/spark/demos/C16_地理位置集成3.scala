package cn.doitedu.spark.demos

import ch.hsr.geohash.GeoHash
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import java.lang

object C16_地理位置集成3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("C16_地理位置集成3").setMaster("local")
    val sc = new SparkContext(conf)

    // 加载日志文件
    val logRDD: RDD[String] = sc.textFile("data/app_log_2021-06-07.log")

    // 加载geohash参考表
    val geoHashRdd = sc.textFile("dataout/ref_geohash").map(s => {
      val arr: Array[String] = s.split(",")
      (arr(0), (arr(1), arr(2), arr(3)))
    })
      // 为了对参考表中可能存在的重复geohash码做去重
      // abcde ： 江西省,上饶市,烤鸡区
      // abcde ： 江西省,上饶市,烤鸭区
      .groupByKey().mapValues(iter => iter.head)

    // 将日志数据rdd变成kv结构便于join
    val kvLogRdd = logRDD.map(s => {
      val jsonObject: JSONObject = JSON.parseObject(s)
      val lat = jsonObject.getDouble("latitude")
      val lng = jsonObject.getDouble("longitude")
      val geohash: String = GeoHash.geoHashStringWithCharacterPrecision(lat, lng, 6)
      (geohash, jsonObject)
    })

    // 将两表join
    val result = kvLogRdd.leftOuterJoin(geoHashRdd).map(tp => {
      val pair: (JSONObject, Option[(String, String, String)]) = tp._2

      val jsonObject: JSONObject = pair._1
      val areaOption: Option[(String, String, String)] = pair._2

      var (province, city, region) = areaOption match {
        case Some((p, c, v)) => (p, c, v)
        case None => ("", "", "")
      }

      // 添加省市区到json对象中
      jsonObject.put("province",province)
      jsonObject.put("city",city)
      jsonObject.put("region",region)

      jsonObject.toJSONString

    })


    // 将处理结果输出到hdfs文件
    result.saveAsTextFile("dataout/areaout")

  }
}
