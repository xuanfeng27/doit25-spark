package cn.doitedu.sparksql.demos

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-09-05
 * @desc
 */
object C16_HIVE分区表操作 {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("多易充满爱")
      .master("local")
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

    val df = rdd.toDF("id", "name", "event", "dt")


    // 写入hive,写成分区表（按dt分区）
    // insert into table event_detail partition(dt='2021-09-05')  select id,name,event  from df   静态分区
    // insert into table event_detail partition(dt) select id,name,event,dt from df   动态分区
    // 这种方式其实属于动态分区
    // df.write.partitionBy("dt").saveAsTable("event_detail")



    val rdd2 = spark.sparkContext.makeRDD(Seq(
      (1, "zs", "addcart", "2021-09-07"),
      (1, "zs", "collect", "2021-09-07"),
      (2, "bb", "addcart", "2021-09-07"),
      (2, "bb", "addcart", "2021-09-07"),
      (2, "bb", "addcart", "2021-09-07"),
      (3, "cc", "addcart", "2021-09-07"),
      (3, "cc", "adclick", "2021-09-07"),
      (2, "bb", "addcart", "2021-09-07"),
    ))

    val df2 = rdd2.toDF("id", "name", "event", "dt")
    // df2.write.mode(SaveMode.Append).partitionBy("dt").saveAsTable("event_detail")


    /**
     * 方式2，不使用api，而是用sql
     */
    val rdd3 = spark.sparkContext.makeRDD(Seq(
      (1, "zs", "addcart", "2021-09-08"),
      (1, "zs", "collect", "2021-09-08"),
      (2, "bb", "addcart", "2021-09-08"),
      (2, "bb", "addcart", "2021-09-08"),
      (2, "bb", "addcart", "2021-09-08"),
      (3, "cc", "addcart", "2021-09-08"),
      (3, "cc", "adclick", "2021-09-08"),
      (2, "bb", "addcart", "2021-09-08"),
    ))

    val df3 = rdd3.toDF("id", "name", "event", "dt")
    df3.createTempView("df3")
    spark.sql(
      """
        |-- 动态分区
        |-- insert into table event_detail partition(dt)
        |-- select
        |--  id,name,event,dt
        |-- from df3
        |
        |-- 静态分区
        |insert into table event_detail partition(dt='2021-09-08')
        |select
        |  id,name,event
        |from df3
        |
        |""".stripMargin)


    spark.close()

  }
}
