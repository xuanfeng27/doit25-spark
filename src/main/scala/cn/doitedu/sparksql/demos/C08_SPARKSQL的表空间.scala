package cn.doitedu.sparksql.demos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalog.Catalog
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-09-04
 * @desc
 *
 * sparksql中有多种表空间
 *    1.  tempView  临时视图表空间
 *    2.  globalTempView 全局临时视图空间  （在多个sparksession之间共享临时视图）
 *    3.  hive元数据空间
 *
 *
 *    如果tempView和hive元数据空间中，有表名相同，则sql查询的是tempView中的
 *    tempView只在创建它的sparksession中可见
 *
 *    globalTempView 在查询时，表名需要加前缀 : select  from global_temp.t_stu
 *    globalTempView在多个sparksession间可以共享
 *
 *
 */
object C08_SPARKSQL的表空间 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache").setLevel(Level.WARN)

    val spark: SparkSession = SparkSession.builder()
      .appName("")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._


    val rdd = spark.sparkContext.makeRDD(Seq(
      (1,"aa"),
      (2,"bb"),
      (3,"cc"),
      (4,"dd")
    ))
    val rddDF: DataFrame = rdd.toDF("id", "name")
    rddDF.createTempView("temp_stu")

    // globaltempview是个奇奇怪怪的小知识
    rddDF.createGlobalTempView("global_temp_stu")
    val spark2: SparkSession = spark.newSession()
    spark2.sql("select * from stu")  // spark2 看不见 临时视图  temp_stu
    spark2.sql("select * from global_temp.global_temp_stu")  // spark2 可以看见globa临时视图 global_temp_stu


    /**
     * 下面的sql查询，将面临一个尴尬的问题： stu到底是哪个stu
     */
    val df: DataFrame = spark.sql(
      """
        |
        |select * from stu
        |
        |""".stripMargin)
    df.show(100,false)


    sys.exit(1)


    // 查询每种性别中，就业薪资最高的两个人的信息
    val df2 = spark.sql(
      """
        |select
        | id,name,gender,salary
        |from
        |  (
        |  select
        |    id,name,gender,salary,row_number() over(partition by gender order by salary desc) as rn
        |  from stu
        |  ) o
        |where rn <= 2
        |
        |""".stripMargin)
    df2.show(100,false)


    spark.close()
  }


}
