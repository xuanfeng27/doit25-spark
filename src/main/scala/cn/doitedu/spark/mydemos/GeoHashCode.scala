package cn.doitedu.spark.mydemos

import ch.hsr.geohash.GeoHash
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{DriverManager, ResultSet}

object GeoHashCode {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("geohash")
    val sc = new SparkContext(conf)
    val getConn = ()=>{
      DriverManager.getConnection(
        "jdbc:mysql://localhost:3306/day01",
        "root",
        "daydayup")
    }
    val sqlStr = "select BD09_LAT,BD09_LNG,area,city,province from area_city_province where BD09_LAT >=? and BD09_LAT <=? "
    val mr = (rs:ResultSet)=>{
        val lat = rs.getDouble(1)
        val lng = rs.getDouble(2)
        val area = rs.getString(3)
        val city = rs.getString(4)
        val province = rs.getString(5)
        val geo = GeoHash.geoHashStringWithCharacterPrecision(lat, lng, 6)
        (geo,area,city,province)
    }
    val rdd: JdbcRDD[(String, String, String, String)] = new JdbcRDD(
      sc,
      getConn,
      sqlStr,
      -90,
      90,
      3,
      mr
    )


    // 1
    rdd.foreachPartition(iter => {
      val conn = getConn()
      val stat = conn.prepareStatement("insert into geo_res values (?,?,?,?)")

      iter.foreach(tp => {
        val (geo, area, city, province) = tp
        stat.setString(1, geo)
        stat.setString(2, area)
        stat.setString(3, city)
        stat.setString(4, province)
        stat.executeUpdate()
      })

      stat.close()
      conn.close()

    })






    // 2
    rdd.map(tp=>tp.productIterator.mkString(",")).saveAsTextFile("hdfs://doit:8020/geo/out")




    sc.stop()

  }
}
