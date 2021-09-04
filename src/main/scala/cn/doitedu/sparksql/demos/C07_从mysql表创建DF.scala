package cn.doitedu.sparksql.demos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object C07_从mysql表创建DF {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache").setLevel(Level.WARN)

    val spark: SparkSession = SparkSession.builder()
      .appName("")
      .master("local")
      .getOrCreate()

    // 映射mysql中的表为dataframe
    val props = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","123456")
    val battle: DataFrame = spark.read.jdbc("jdbc:mysql://localhost:3306/abc","battel","id",1,100,2,props)
    val battle_info: DataFrame = spark.read.jdbc("jdbc:mysql://localhost:3306/abc","battel_info","id",1,100,2,props)

    battle.createTempView("bat")
    battle_info.createTempView("info")

    val res = spark.sql(
      """
        |
        |select
        |  a.id,
        |  a.name,
        |  a.role,
        |  a.battel,
        |  b.phone,
        |  b.age
        |from bat a join info b
        |on a.id=b.id
        |
        |""".stripMargin)

    res.show(100,false)

    spark.close()

  }
}
