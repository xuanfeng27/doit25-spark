package cn.doitedu.sparksql.mydemos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Day01_2 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache").setLevel(Level.WARN)

    val spark = SparkSession
      .builder()
      .appName("day02")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()


    //求 每种性别中 ，总收入最高的前3个人的信息（id，name，gender，总收入）
    val df = spark.sql(
      """
        |select
        |id,name,gender,sumSalary
        |from
        |(
        |select
        |id,name,gender,sumSalary,
        |row_number() over(partition by gender order by sumSalary desc) as rn
        |from
        |(
        |select
        |id,
        |gender,
        |name,
        |sum(salary) as sumSalary
        |from
        |tab_spark
        |group by name,id,gender
        |)t1
        |)t2
        |where rn<=3
        |""".stripMargin)

    df.show()

    spark.close()
  }
}
