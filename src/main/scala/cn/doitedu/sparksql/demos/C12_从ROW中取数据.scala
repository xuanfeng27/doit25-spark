package cn.doitedu.sparksql.demos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}


/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-09-05
 * @desc
 */
object C12_从ROW中取数据 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark: SparkSession = SparkSession.builder()
      .appName("C10_DF上的DSL风格API")
      .config("spark.sql.crossJoin.enabled", "true")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

    val schema = StructType(Seq(
      StructField("id", DataTypes.IntegerType),
      StructField("name", DataTypes.StringType),
      StructField("role", DataTypes.StringType),
      StructField("energy", DataTypes.DoubleType)
    ))


    val df = spark.read.schema(schema).options(Map("header" -> "true")).csv("data/battel2.txt")


    // 获取数据方式1： getAs[T](字段名)
    df.rdd.map(row => {
      val id = row.getAs[Int]("id")
      val name = row.getAs[String]("name")
      (id,name)
    })



    // 获取数据方式2： getAs[T](字段索引号)
    df.rdd.map(row => {
      row.getAs[Int](0) // 索引从 0 开始
    }).foreach(println)


    // 获取数据方式3： getInt(字段索引号)
    df.rdd.map(row => {
      row.getInt(0)
    }).foreach(println)

    // 获取数据方式4： getInt(字段索引号)
    df.rdd.map(row => {
      // 不指定类型
      val idAny: Any = row.get(0)
      // 自己强转
      idAny.asInstanceOf[Int]
    }).foreach(println)



    // 获取数据方式5： 模式匹配
    df.rdd.map(row => {
      row match {
        case Row(id: Int, name: String, role: String, energy: Double) =>  (id, name, role, energy)
        case _ => (-1, "", "", 0.0)
      }
    }).foreach(println)


    // 获取数据方式6： 模式匹配-->偏函数写法
    df.rdd.map {
      case Row(id: Int, name: String, role: String, energy: Double) => (id, name, role, energy)
    }


    spark.close()
  }
}
