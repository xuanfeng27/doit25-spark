package cn.doitedu.sparksql.demos

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SparkSession}

object C01_SPARKSQL入门示例 {
  def main(args: Array[String]): Unit = {

    // sqlContext  sparkContext
    val spark: SparkSession = SparkSession.builder()
      .appName("C01_SPARKSQL入门示例")
      .master("local")
      .getOrCreate()

    // 可以从sparksession中取到sparkContext
    val sc: SparkContext = spark.sparkContext
    // 可以从sparksession中取到sqlContext
    val sqlContext: SQLContext = spark.sqlContext

    // 在新版中，不需要将sparkContext和SqlContext割裂使用，而是统一使用sparksession来构建计算逻辑即可

    // 加载源数据文件为dataframe
    val df1: Dataset[Row] = spark.read.csv("data/battel.txt")
    val df2: DataFrame = df1.toDF("id", "name", "role", "energy")
    // 查看df的schema信息
    df2.printSchema()
    /**
     * root
        |-- id: string (nullable = true)
        |-- name: string (nullable = true)
        |-- role: string (nullable = true)
        |-- energy: string (nullable = true)
     */

    // 查看df的数据和schema信息
    df2.show()

    /**
     * 用调API的风格表达sql逻辑
     */
    // 查询所有战斗力大于 400的记录
    val res1: DataFrame = df2.where("energy > 400")
    res1.show()

    // 查询每一种角色的平均战斗力
    val res2: DataFrame = df2.groupBy("role").agg("energy" -> "avg")
    res2.show()


    /**
     * 原汁原味的写sql
     */
    df2.createTempView("battel")   // 注册一个临时表（视图）名
    spark.sql(
      """
        |
        |select
        |  role,
        |  avg(energy) as avg_energy
        |from battel
        |where energy >= 400
        |group by role
        |
        |""".stripMargin).show()

    spark.close()
  }
}
