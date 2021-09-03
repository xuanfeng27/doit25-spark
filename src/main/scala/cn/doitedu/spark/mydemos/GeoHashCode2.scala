package cn.doitedu.spark.mydemos

import ch.hsr.geohash.GeoHash
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GeoHashCode2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("geohash2")
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.textFile("data/app_log_2021-06-07.log")
    val rdd1: RDD[(String, JSONObject)] = rdd.map(str => {
      val jb = JSON.parseObject(str)
      val lat = jb.getDouble("latitude")
      val lng = jb.getDouble("longitude")
      val geo: String = GeoHash.geoHashStringWithCharacterPrecision(lat, lng, 6)
      (geo, jb)
    })

    val rddGeo: RDD[String] = sc.textFile("hdfs://doit:8020/geo/out/")
    val rddGeo2: RDD[(String, (String, String, String))] = rddGeo.map(str => {
      val arr = str.split(",")
      (arr(0),(arr(1), arr(2), arr(3)) )
    })

    val rddRes: RDD[(String, (JSONObject, Option[(String, String, String)]))] = rdd1.leftOuterJoin(rddGeo2)

    val res: RDD[String] = rddRes.map(tp => {
      val jb = tp._2._1
      val opt = tp._2._2
      if (opt.isDefined) {
        val (area, city, province) = opt.get
        jb.put("area", area)
        jb.put("city", city)
        jb.put("province", province)
      }else{
        jb.put("area", "")
        jb.put("city", "")
        jb.put("province", "")
      }
      jb.toJSONString
    })

    res.saveAsTextFile("dataout/geo")



    sc.stop()
  }
}
