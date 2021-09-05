package cn.doitedu.sparksql.demos

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import scala.collection.mutable

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-09-05
 * @desc 自定义函数
 * 案例需求： 计算数据中所有人的两两之间余弦相似度
 */
object C17_自定义函数 {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("余弦相似度")
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


    // 为了方便计算两人之间的余弦相似度，可以把表做自连接（笛卡尔）
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

    val cosSimilarity = (arr1:mutable.WrappedArray[Double], arr2:mutable.WrappedArray[Double] )=>{

      val fenmu1: Double = Math.pow(arr1.map(Math.pow(_, 2)).sum, 0.5)
      val fenmu2: Double = Math.pow(arr2.map(Math.pow(_, 2)).sum, 0.5)

      val fenzi: Double = arr1.zip(arr2).map(tp => tp._1 * tp._2).sum

      fenzi/(fenmu1*fenmu2)

    }
    // 注册函数
    spark.udf.register("cos_sim",cosSimilarity)


    val res = spark.sql(
      """
        |
        |select
        | id,
        | name,
        | bid,
        | bname,
        | cos_sim(features_a,features_b) as cos_sim
        |
        |from joined
        |
        |""".stripMargin)

    res.createTempView("res")

    // 统计出每个人最相似的两个人是谁
    /**
     * +---+----+---+-----+------------------+
       |id |name|bid|bname|cos_sim           |
       +---+----+---+-----+------------------+
       |1  |a   |2  |b    |0.999139100220926 |
       |1  |a   |3  |c    |0.9964006907870342|
       |1  |a   |4  |d    |0.9994977524438583|
       |1  |a   |5  |e    |0.9991691603657344|
       |1  |a   |6  |f    |0.9966855393508363|
       |1  |a   |7  |g    |0.9999455877069451|
       |2  |b   |1  |a    |0.999139100220926 |
       |2  |b   |3  |c    |0.9975151222348045|
       |2  |b   |4  |d    |0.9988019757523202|
       |2  |b   |5  |e    |0.9995271267120339|
       |2  |b   |6  |f    |0.9972087920927727|
     */
    spark.sql(
      """
        |select
        |  id,bid,cos_sim
        |from (
        |  select
        |  id,bid,cos_sim,row_number() over(partition by id order by cos_sim desc) as rn
        |  from res
        |) o
        |where rn<=2
        |
        |""".stripMargin).show(100,false)



    // TODO  用欧几里得距离再来实现一遍上面的需求



    spark.close()

  }
}
