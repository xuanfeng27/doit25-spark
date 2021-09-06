package cn.doitedu.sparksql.mydemos

import org.apache.spark.sql.SparkSession

object Day02_1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("day02_1 from hbase")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()


    spark.sql(
      """
        |select
        |*
        |from
        |
        |""".stripMargin).show(100,false)





    spark.close()
  }
}
