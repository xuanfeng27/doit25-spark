package cn.doitedu.sparksql.demos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-09-04
 * @desc sql也能处理非结构化的数据
 */
object C05_从非结构化文件创建DF {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache").setLevel(Level.WARN)

    val spark: SparkSession = SparkSession.builder()
      .appName("")
      .master("local")
      .getOrCreate()


    val df: DataFrame = spark.read.text("data/wordcount.txt")
    //df.printSchema()
    //df.show(100,false)
    /*
        +-------------------------------------------------------+
        |value                                                  |
        +-------------------------------------------------------+
        |doit edu doitedu doit doit doit edu                    |
        |doit edu has a greate   man his name is deep as the sea|
        |spark spark spark                                      |
        |flink flink                                            |
        |greate                                                 |
        +-------------------------------------------------------+
    */
    df.createTempView("wc")
    spark.sql(
      """
        |select
        |  word,
        |  count(1) as cnt
        |from
        |  (
        |     select
        |       explode(split(value,'\\s+')) as word
        |     from wc
        |  ) o
        |group by word
        |
        |""".stripMargin).show(100,false)

    spark.close()

  }

}
