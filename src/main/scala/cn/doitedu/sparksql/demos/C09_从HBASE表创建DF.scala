package cn.doitedu.sparksql.demos

import org.apache.spark.sql.SparkSession

object C09_从HBASE表创建DF {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("C09_从HBASE表创建DF")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()



    spark.sql(
      """
        |
        |select
        |  *
        |from doitedu_stu
        |
        |""".stripMargin).show(100,false)


    spark.close()

  }

}
