package cn.doitedu.sparksql.mydemos

import org.apache.spark.sql.SparkSession

object Day02_partition {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("充满爱")
      .master("local")
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val rdd = spark.sparkContext.makeRDD(Seq(
      (1, "zs", "addcart", "2021-09-05"),
      (1, "zs", "collect", "2021-09-05"),
      (1, "zs", "pageview", "2021-09-05"),
      (2, "bb", "addcart", "2021-09-05"),
      (2, "bb", "addcart", "2021-09-05"),
      (2, "bb", "addcart", "2021-09-06"),
      (3, "cc", "adshow", "2021-09-06"),
      (3, "cc", "addcart", "2021-09-06"),
      (3, "cc", "adclick", "2021-09-06"),
      (2, "bb", "addcart", "2021-09-06"),
    ))

    val df = rdd.toDF("id", "name", "eventId", "dt")
    //动态分区
    //df.write.partitionBy("dt").saveAsTable("tab_event")


    df.createTempView("df")
/*    spark.sql(
      """
        |--动态分区
        |insert into tab_eventId
        |partition (dt)
        |select
        |id,name,eventId,dt
        |from
        |df
        |""".stripMargin)*/


    val rdd2 = spark.sparkContext.makeRDD(Seq(
      (1, "zs", "addcart", "2021-09-08"),
      (1, "zs", "collect", "2021-09-08"),
      (1, "zs", "pageview", "2021-09-08"),
      (2, "bb", "addcart", "2021-09-08")
    ))
    val df2 = rdd2.toDF("id", "name", "eventId", "dt")
    df2.createTempView("df2")
    spark.sql(
      """
        |--静态分区
        |insert into tab_eventId
        |partition (dt="2021-09-08")
        |select
        |id,name,eventId
        |from
        |df2
        |""".stripMargin)


    spark.close()
  }
}
