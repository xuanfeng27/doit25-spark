package cn.doitedu.sparksql.demos

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-09-05     
 * @desc 输出dataframe
 */
object C15_输出Dataset {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("多易教育充满爱")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._


    val rdd = spark.sparkContext.makeRDD(Seq(
      (10, "aa", 18),
      (11, "bbbb", 28),
      (12, "cc", 38),
      (13, "dddd", 28),
    ))

    // 转成dataset
    val df: DataFrame = rdd.toDF("id", "name", "age")


    /**
     * 一， 打印
     */
    // df.show(100,false)


    /**
     * 二 ，保存为文件
     */
    // writeFile(df)


    /**
     * 三 ，保存到mysql数据库
     */
    // writeToMysql(df)


    /**
     * 四 ，保存到  hive
     *
     */
    df.write.mode(SaveMode.Append).saveAsTable("teacher")



    spark.close()
  }

  def writeFile(df: DataFrame): Unit = {
    // 保存为parquet文件
    df.write.parquet("dataout/dfout/paruqet/")

    // 保存为csv文件
    df.write.csv("dataout/dfout/csv/")

    // 保存为json文件
    df.write.json("dataout/dfout/json/")

    // 保存为orc文件
    df.write.orc("dataout/dfout/orc/")

    // 写成非机构化的文本，需要将表结构变成1个字符串字段
    // df.write.text("dataout/dfout/text/")
    df.selectExpr("concat_ws(',',id,name,age)").write.text("dataout/dfout/text")
  }


  def writeToMysql(df: DataFrame): Unit = {
    val props = new Properties()
    props.load(this.getClass.getClassLoader.getResourceAsStream("mysql.properties"))
    // 写入mysql时，有多种保存模式，默认是： SaveMode: ErrorIfExists
    // df.write.jdbc(props.getProperty("url"),"teacher",props)

    // 可以通过mode方法，来设置所需的存储模式
    // overwrite 是把原来的表数据全部清空，然后写入本次的数据
    df.write.mode(SaveMode.Overwrite).jdbc(props.getProperty("url"), "teacher", props)

    // append追加模式
    df.write.mode(SaveMode.Append).jdbc(props.getProperty("url"), "teacher", props)

    // ignore如果表存在，则忽略本次存储动作
    df.write.mode(SaveMode.Ignore).jdbc(props.getProperty("url"), "teacher", props)

    df.write.mode(SaveMode.ErrorIfExists).jdbc(props.getProperty("url"), "teacher", props)

  }


}
