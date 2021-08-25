package cn.doitedu.spark.demos

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, ResultSet}
import scala.collection.mutable.ArrayBuffer

object WordCount {

  def main(args: Array[String]): Unit = {

 /*   val conf = new SparkConf()
      .setMaster("local")
      .setAppName(WordCount.getClass.getName)

    val sc = new SparkContext(conf)

    sc.textFile("data/wordcount.txt",2)
      .flatMap(_.split("\\s+"))
      .map(word=>(word,1))
      .reduceByKey((x,y)=>x+y)
      .foreach(println)

    sc.stop()*/


    //jdbcrdd
    val context = new SparkContext(new SparkConf().setMaster("local").setAppName("jdbcTest"))

    val conn: () => Connection = ()=>{
      val connection = DriverManager
        .getConnection(
          "jdbc:mysql://localhost:3306/day01",
          "root",
          "daydayup"
        )
      connection
    }

    val sql = "select id,name,role,battel from battel where id >= ? and id <=?"

    val res = (resultSet:ResultSet)=>{
        val id = resultSet.getInt(1)
        val name = resultSet.getString(2)
        val role = resultSet.getString(3)
        val battel = resultSet.getInt(4)
      (id,name,role,battel)
    }

    val rdd = new JdbcRDD[(Int, String, String, Int)](
      context,
      conn,
      sql,
      1,
      6,
      1,
      res
    )

    rdd.foreach(println)
  }
}
