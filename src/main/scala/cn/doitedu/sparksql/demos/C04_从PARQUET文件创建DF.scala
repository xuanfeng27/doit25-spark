package cn.doitedu.sparksql.demos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object C04_从PARQUET文件创建DF {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache").setLevel(Level.WARN)

    val spark: SparkSession = SparkSession.builder()
      .appName("")
      .master("local")
      .getOrCreate()


    // parquet文件是自我描述的，带完整schema信息，spark解析出来就能得到正确的schema，所以不用自己传递schema
    val df: DataFrame = spark.read.parquet("data/parquetdemo")
    df.printSchema()
    df.show()


    // spark也可以解析orc文件（hive中最常用的列式存储格式文件）
    val df2: DataFrame = spark.read.orc("")


    spark.close()

  }

}
