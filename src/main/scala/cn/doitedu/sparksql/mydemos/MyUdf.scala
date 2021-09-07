package cn.doitedu.sparksql.mydemos

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import scala.collection.mutable

object MyUdf{
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("oshi")
      .master("local[*]")
      .getOrCreate()


    // 加载特征数据
    // id,name,age,height,weight,facevalue,score
    val schema = StructType(Seq(
      StructField("id",DataTypes.IntegerType),
      StructField("name",DataTypes.StringType),
      StructField("age",DataTypes.DoubleType),
      StructField("height",DataTypes.DoubleType),
      StructField("weight",DataTypes.DoubleType),
      StructField("facevalue",DataTypes.DoubleType),
      StructField("score",DataTypes.DoubleType)
    ))
    val df = spark.read.option("header","true").schema(schema).csv("data/features.txt")


    // 可以把表做自连接（笛卡尔）
    df.createTempView("df")
    val joined = spark.sql(
      """
        |
        |select
        |   a.id,
        |   a.name,
        |   b.id as bid,
        |   b.name as bname,
        |   array(a.age,a.height,a.weight,a.facevalue,a.score) as features_a,
        |   array(b.age,b.height,b.weight,b.facevalue,b.score) as features_b
        |from df a join df b on a.id != b.id
        |
        |""".stripMargin)
    joined.createTempView("joined")

    val oFun = (arr1:mutable.WrappedArray[Double], arr2:mutable.WrappedArray[Double] )=>{
      1/Math.pow(arr1.zip(arr2).map(tp => Math.pow(tp._1-tp._2,2)).sum,0.5)
    }
    // 注册函数
    spark.udf.register("ofun",oFun)

    spark.sql(
      """
        |select
        | id,
        | name,
        | bid,
        | bname,
        | oFun(features_a,features_b) as ofun
        |from joined
        |""".stripMargin).show()


    spark.close()
  }
}
