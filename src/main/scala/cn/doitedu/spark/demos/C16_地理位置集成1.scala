package cn.doitedu.spark.demos

import ch.hsr.geohash.GeoHash
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-09-02
 * @desc 将地理位置参考点表，转成geohash码参考表
 */
object C16_地理位置集成1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("地理位置集成").setMaster("local")
    val sc = new SparkContext(conf)

    // 读取mysql表
    val getConn = ()=> DriverManager.getConnection("jdbc:mysql://localhost:3306/realtimedw?useUnicode=true&characterEncoding=UTF8","root","123456")
    val sql:String = "select lat,lng,province,city,region from ref_zb where lat>= ? and lat <= ? "
    val mapRow = (rs:ResultSet) => {
      val lat = rs.getDouble(1)
      val lng = rs.getDouble(2)
      val province = rs.getString(3)
      val city = rs.getString(4)
      val region = rs.getString(5)

      val geohash = GeoHash.geoHashStringWithCharacterPrecision(lat,lng,6)

      (geohash,province,city,region)
    }
    val jdbcRDD = new JdbcRDD[(String, String, String, String)](sc, getConn, sql, -100, 100, 2, mapRow)

    // 1. 写回mysql
    /*jdbcRDD.foreachPartition(iter=>{
      // 建数据库连接
      val conn  = getConn()
      val stmt: PreparedStatement = conn.prepareStatement("insert into ref_geohash values (?,?,?,?)")

      // 迭代数据插入mysql
      iter.foreach(tp=>{
        stmt.setString(1,tp._1)
        stmt.setString(2,tp._2)
        stmt.setString(3,tp._3)
        stmt.setString(4,tp._4)

        stmt.executeUpdate()
        println("====== 执行了 ===========")
      })
      // 关闭连接
      stmt.close()
      conn.close()
    })*/

    // 2. 写入hdfs文件
    jdbcRDD.map(tp=>tp.productIterator.mkString(",")).saveAsTextFile("dataout/ref_geohash")



    sc.stop()
  }
}
