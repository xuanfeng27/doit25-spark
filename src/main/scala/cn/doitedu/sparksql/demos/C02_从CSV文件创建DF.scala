package cn.doitedu.sparksql.demos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object C02_从CSV文件创建DF {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)

    val spark: SparkSession = SparkSession.builder()
      .appName("")
      .master("local")
      .getOrCreate()


    // 从不带表头的CSV文件创建DF : （1）
    println("========= df1 ============")
    val df1: DataFrame = spark.read.csv("data/battel.txt")
    //df1.printSchema()
    /**
     * root
        |--  _c0: string (nullable = true)
        |--  _c1: string (nullable = true)
        |--  _c2: string (nullable = true)
        |--  _c3: string (nullable = true)
     */


    // 从不带表头的CSV文件创建DF : （2）  为字段重命名
    println("========= df2 ============")
    val df2: DataFrame = spark.read.csv("data/battel.txt").toDF("id","name","role","energy")
    df2.printSchema()
    /**
     * root
         |-- id: string (nullable = true)
         |-- name: string (nullable = true)
         |-- role: string (nullable = true)
         |-- energy: string (nullable = true)
     */



    // 从不带表头的CSV文件创建DF : （3）  让spark自动推断字段类型
    // 设置一个option选项即可自动推断字段类型： inferSchema=true
    // 不要在实际生产中这么用，因为推断字段类型需要单独触发job
    println("========= df3 ============")
    val df3: DataFrame = spark.read.option("inferSchema","true").csv("data/battel.txt").toDF("id","name","role","energy")
    df3.printSchema()
    /**
       root
          |-- id: integer (nullable = true)
          |-- name: string (nullable = true)
          |-- role: string (nullable = true)
          |-- energy: integer (nullable = true)
     */


    // 从不带表头的CSV文件创建DF : （4）  且自定义schema
    println("========= df4 ============")
    val schema = StructType(Seq(
      StructField("id",DataTypes.IntegerType),
      StructField("name",DataTypes.StringType),
      StructField("role",DataTypes.StringType),
      StructField("energy",DataTypes.DoubleType)
    ))
    val df4: DataFrame = spark.read.schema(schema).csv("data/battel.txt")
    df4.printSchema()
    //df4.write.parquet("data/parquetdemo/")
    /**
     * root
        |-- id: integer (nullable = true)
        |-- name: string (nullable = true)
        |-- role: string (nullable = true)
        |-- energy: double (nullable = true)
     */



    // 从带表头的CSV文件创建DF : （5）
    // 设置一个option选项： header=true，则会将文件的第一行认作“表头”，不要当成数据
    // 正确的schema还是需要自己传入
    println("========= df5 ============")
    val df5: DataFrame = spark.read.option("header","true").csv("data/battel2.txt")
    df5.printSchema()
    df5.show()



    spark.close()



  }
}
