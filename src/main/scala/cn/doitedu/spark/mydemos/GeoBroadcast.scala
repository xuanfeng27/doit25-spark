package cn.doitedu.spark.mydemos

import ch.hsr.geohash.GeoHash
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object GeoBroadcast {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("GeoBroadcast")
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.textFile("data/app_log_2021-06-07.log")
    val rddLog: RDD[(String, JSONObject)] = rdd.map(str => {
      val jb = JSON.parseObject(str)
      val lat = jb.getDouble("latitude")
      val lng = jb.getDouble("longitude")
      val geo: String = GeoHash.geoHashStringWithCharacterPrecision(lat, lng, 6)
      (geo, jb)
    })


    val rddGeo: Map[String, (String, String, String)] =
      sc.textFile("hdfs://doit:8020/geo/out/")
        .map(str => {
          val arr = str.split(",")
          (arr(0),(arr(1),arr(2), arr(3)) )
        }).toLocalIterator.toMap

    val bc: Broadcast[Map[String, (String, String, String)]] = sc.broadcast(rddGeo)

    rddLog.map(tp=>{
      val data: Map[String, (String, String, String)] = bc.value
      val option = data.get(tp._1)
      var (area,city,province) = option match {
        case Some((a, b, c)) => (a, b, c)
        case None => ("", "", "")
      }
      val jb = tp._2
      jb.put("area",area)
      jb.put("city",city)
      jb.put("province",province)
      jb.toJSONString
    })
      .saveAsTextFile("dataout/outbc")

    sc.stop()
  }
}
