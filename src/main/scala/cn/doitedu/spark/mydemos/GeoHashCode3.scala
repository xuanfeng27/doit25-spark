package cn.doitedu.spark.mydemos

import ch.hsr.geohash.GeoHash
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import java.sql.DriverManager

object GeoHashCode3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("geohash3")
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.textFile("data/app_log_2021-06-07.log")
    val rddlog: RDD[(String, JSONObject)] = rdd.map(str => {
      val jb = JSON.parseObject(str)
      val lat = jb.getDouble("latitude")
      val lng = jb.getDouble("longitude")
      val geo: String = GeoHash.geoHashStringWithCharacterPrecision(lat, lng, 6)
      (geo, jb)
    })


    //
    val res: RDD[String] = rddlog.mapPartitions(iter => {
      val conn = DriverManager.getConnection(
        "jdbc:mysql://localhost:3306/day01?useUnicode=true&characterEncoding=utf8",
        "root",
        "daydayup"
      )
      val stat = conn.prepareStatement("select area,city,province from geo_res where geo=?")


      iter.map(tp => {
        val (geo, jb) = tp
        stat.setString(1, geo)
        val resultSet = stat.executeQuery()
        var (area, city, province) = ("", "", "")
        if (resultSet.next()) {
          area = resultSet.getString(1)
          city = resultSet.getString(2)
          province = resultSet.getString(3)
        }
        jb.put("area", area)
        jb.put("city", city)
        jb.put("province", province)
        jb.toJSONString
      })


    })

    res.saveAsTextFile("dataout/geosql")

    sc.stop()
  }
}
