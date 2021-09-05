package cn.doitedu.sparksql.demos

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-09-05
 * @desc 前面所学的dataframe其实就是dataset[Row]
 * dataset的各类操作和dataframe是一模一样的，就是里面的类型不是Row了
 */

case class Soldier(id:Int,name:String,role:String,energy:Double)

object C13_DATASET操作API {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("我爱你")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val ds: Dataset[String] = spark.read.textFile("data/battel.txt")
    val df: Dataset[Row] = ds.toDF()

    // 对dataset[T]做map,flatMap等rdd算子操作，是可以再次返回一个dataset[U]的
    val ds2: Dataset[(Int, String, String, Double)] = ds.map(s=>{
      val arr: Array[String] = s.split(",")
      (arr(0).toInt,arr(1),arr(2),arr(3).toDouble)
    })

    // 对dataset[T]做map,flatMap等rdd算子操作，是可以再次返回一个dataset[U]的
    val ds3: Dataset[Soldier] = ds2.map(tp=> Soldier(tp._1,tp._2,tp._3,tp._4))

    ds3.createTempView("ds3")
    val res: Dataset[Row] = spark.sql(
      """
        |
        |select
        |  role,
        |  sum(energy) as amt_energy
        |from ds3
        |group by role
        |
        |""".stripMargin)


    // 但是，只要一select，就必然退化成 dataset[Row] ：dataframe
    val ds4: DataFrame = ds2.select("_1","_2","_3")

    spark.close()
  }

}
