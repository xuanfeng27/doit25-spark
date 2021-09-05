package cn.doitedu.sparksql.demos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object C11_DF转成RDD写底层代码 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark: SparkSession = SparkSession.builder()
      .appName("C10_DF上的DSL风格API")
      .config("spark.sql.crossJoin.enabled","true")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

    val schema = StructType(Seq(
      StructField("id", DataTypes.IntegerType),
      StructField("name", DataTypes.StringType),
      StructField("role", DataTypes.StringType),
      StructField("energy", DataTypes.DoubleType)
    ))


    val df = spark.read.schema(schema).options(Map("header" -> "true")).csv("data/battel2.txt")


    df.createTempView("df")
    val df2: Dataset[Row] = spark.sql(
      """
        |
        |select
        | role,max(energy) as max_energy
        |from df
        |group by role
        |
        |""".stripMargin)


    // 假设后续的一些计算逻辑不再方便用sql表达，可以把dataframe转成rdd来写底层代码

    // 一、  直接在dataset上掉map算子，会返回一个dataset，因而需要一个返回dataset[U]的对应Encoder[U]
    // 提供这个Encoder有两种方式：
    // 方式1： 导入隐式上下文，通过上下文中的隐式转换得到 Encoder[U]
    import spark.implicits._
    val ds: Dataset[(String, Double)] = df2.map(row=>{
      val role: String = row.getAs[String]("role")
      val max_energy: Double = row.getAs[Double]("max_energy")

      (role,max_energy)
    })


    // 方式2： 显式传入Encoder[U]
    df2.map(row=>{
      val role: String = row.getAs[String]("role")
      val max_energy: Double = row.getAs[Double]("max_energy")
      (role,max_energy)
    })(Encoders.product)


    //  二、  从 dataset取到rdd，然后调map/flatmap等算子
    val rdd2: RDD[(String, Double)] = df2.rdd.map(row=>{
      val role: String = row.getAs[String]("role")
      val max_energy: Double = row.getAs[Double]("max_energy")
      (role,max_energy)
    })

  }

}
