package cn.doitedu.myredis

import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import redis.clients.jedis.{GeoRadiusResponse, GeoUnit, Jedis, Response}

import java.util

object DemoGeoLog {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder.master("local").appName("geo位置").getOrCreate
    import spark.implicits._
    val ds: Dataset[String] = spark.read.textFile("data/app_log_2021-06-07.log")

    val rdd = ds.rdd
    val rs = rdd.mapPartitions(iter => {

      val jedis = new Jedis("doit", 6379)

      iter.map(line => {
        val jSONObject = JSON.parseObject(line)
        val lng = jSONObject.getDouble("longitude")
        val lat = jSONObject.getDouble("latitude")

        val res: util.List[GeoRadiusResponse] = jedis.georadius("geo:info", lng, lat, 5, GeoUnit.KM)
        if (null != res && res.size() > 0) {
          val str = res.get(0).getMemberByString
          val arr = str.split(",")
          jSONObject.put("province", arr(0))
          jSONObject.put("city", arr(1))
          jSONObject.put("area", arr(2))
        }

        jSONObject.toJSONString
      })


    })


    rs.saveAsTextFile("dataout/geoJedis")


    spark.close()






  }
}
