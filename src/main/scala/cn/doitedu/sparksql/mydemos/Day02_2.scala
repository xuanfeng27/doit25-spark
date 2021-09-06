package cn.doitedu.sparksql.mydemos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, ColumnName, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import java.util.Properties

object Day02_2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark = SparkSession
      .builder()
      .appName("test")
      .master("local")
      .config("spark.sql.crossJoin.enabled","true")
      .enableHiveSupport()
      .getOrCreate()

    val schema = StructType(Seq(
      StructField("id", DataTypes.IntegerType),
      StructField("name", DataTypes.StringType),
      StructField("role", DataTypes.StringType),
      StructField("energy", DataTypes.DoubleType)
    ))
    val df = spark.read
      .schema(schema)
      .options(Map("header" -> "true"))
      .csv("data/battel2.txt")

    df.select("id","name","role")
    val idcolumn: Column = df("id")
    df.select(idcolumn,df("name"))


    import spark.implicits._

    val idcol: ColumnName = $"id"
    df.select(idcol,$"name")
    df.select('id,'name,'role)

    import org.apache.spark.sql.functions._
    val col1: Column = col("id")
    df.select(col1,col("name"))

    df.selectExpr("id","upper(name) as up","role")
    df.select('id,upper('name),'role)


    df.where("id >3")
    df.where('id >3)


    df.groupBy("role").sum("energy")
    df.groupBy("role").agg(
      sum("energy").as("energy_sum"),
      max("energy").as("max_energy")
    )


    df.orderBy("energy","id")
    df.orderBy('energy desc,'id)


    df.selectExpr(
      "id",
      "name",
      "role",
      "energy",
      "row_number() over(partition by role order by energy desc) as rn"
    ).where("rn<2")



    val props = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","daydayup")
    val df2 = spark.read.jdbc(
      "jdbc:mysql://localhost:3306/day01?useUnicode=true&characterEncoding=UTF8",
      "battel",
      props
    )

    df.join(df2)//spark.sql.crossJoin.enabled=true;
    df.join(df2,df("id")===df2("id"))


    df.union(df2)


    df.createTempView("dfview")
    val df3 = spark.sql(
      """
        |select
        |id,name,role
        |from
        |dfview
        |""".stripMargin)




    //
    val rdd: RDD[(Int, String)] = df.rdd.map(row => {
      val id: Int = row.getAs[Int]("id")
      val name = row.getAs[String]("name")
      (id, name)
    })



    //writeToMysql
    val prop = new Properties()
    prop.load(this.getClass.getClassLoader.getResourceAsStream("zllsql.properties"))
    df.write.mode(SaveMode.Append).jdbc(prop.getProperty("url"),"battel_test",prop)




    //保存到  hive
    df.write.mode(SaveMode.Append).saveAsTable("battel_save")


  spark.close()

  }
}
