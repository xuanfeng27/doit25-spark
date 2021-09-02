package cn.doitedu.spark.demos

import ch.hsr.geohash.GeoHash
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import java.lang
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

object C16_地理位置集成2 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("地理位置集成2").setMaster("local")
    val sc = new SparkContext(conf)

    // 加载日志文件
    val logRDD: RDD[String] = sc.textFile("data/app_log_2021-06-07.log")



    // 集成省市区地理位置信息
    val areaAddedRDD = logRDD.mapPartitions(iter => {
      // 建数据库连接
      val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/realtimedw?useUnicode=true&characterEncoding=UTF8", "root", "123456")
      val stmt: PreparedStatement = conn.prepareStatement("select province,city,region from ref_geohash where geohash = ?")

      // 迭代日志，逐条查询
      iter.map(str => {

        // 将json串解析成jsonobjet对象
        val jsonObject: JSONObject = JSON.parseObject(str)

        val lat: lang.Double = jsonObject.getDouble("latitude")
        val lng: lang.Double = jsonObject.getDouble("longitude")
        // 把经纬度坐标转换成geohash编码
        val geohash: String = GeoHash.geoHashStringWithCharacterPrecision(lat, lng, 6)

        // 拿着日志中的geohash编码去参考表中查询
        stmt.setString(1,geohash)
        val rs: ResultSet = stmt.executeQuery()

        // 获取参考表的查询结果，并为省市区赋值
        var Array(province, city, region) = Array("", "", "")
        if (rs.next()) {
          province = rs.getString("province")
          city = rs.getString("city")
          region = rs.getString("region")
        }

        // 将查询到的省市区添加到json中
        jsonObject.put("province", province)
        jsonObject.put("city", city)
        jsonObject.put("region", region)

        // 将添加了省市区后的json对象，转成json字符串
        jsonObject.toJSONString

      })
    })


    // 保存处理结果
    areaAddedRDD.saveAsTextFile("dataout/aread/")


    sc.stop()
  }
}
