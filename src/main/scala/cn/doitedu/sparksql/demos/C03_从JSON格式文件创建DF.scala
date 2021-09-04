package cn.doitedu.sparksql.demos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object C03_从JSON格式文件创建DF {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache").setLevel(Level.WARN)

    val spark: SparkSession = SparkSession.builder()
      .appName("")
      .master("local")
      .getOrCreate()


    // 构造一个自定义的schema
    val schema = StructType(Seq(
      StructField("id", DataTypes.IntegerType),
      StructField("name", DataTypes.StringType),
      StructField("role", DataTypes.StringType),
      StructField("energy", DataTypes.DoubleType),
    ))


    val df: DataFrame = spark.read.schema(schema).json("data/battel_json.txt")
    df.printSchema()
    df.show()


    /**
     * 复杂json示例
     */
    val df2: DataFrame = spark.read.json("data/app_log_2021-06-07.log")
    df2.printSchema()
    df2.show(50, false)
    df2.selectExpr("properties.couponId")

    /**
     * json中的嵌套字段： properties，被解析生了sql中的  StructType
     * |-- properties: struct (nullable = true)
     * |-- account: string (nullable = true)
     * |-- adCampain: string (nullable = true)
     * |-- adId: string (nullable = true)
     * |-- adLocation: string (nullable = true)
     * ..........
     * 而每一行中的properties中的属性字段其实并不完全一样，导致结构很冗余
     *
     * 可以自己定义schema，把properties字段定义成 Map类型
     */

    val schema2 = StructType(Seq(
      StructField("account", DataTypes.StringType),
      StructField("appId", DataTypes.StringType),
      StructField("appVersion", DataTypes.StringType),
      StructField("carrier", DataTypes.StringType),
      StructField("deviceId", DataTypes.StringType),
      StructField("deviceType", DataTypes.StringType),
      StructField("eventId", DataTypes.StringType),
      StructField("ip", DataTypes.StringType),
      StructField("latitude", DataTypes.DoubleType),
      StructField("longitude", DataTypes.DoubleType),
      StructField("netType", DataTypes.StringType),
      StructField("osName", DataTypes.StringType),
      StructField("osVersion", DataTypes.StringType),
      StructField("properties", DataTypes.createMapType(DataTypes.StringType,DataTypes.StringType)),
      StructField("releaseChannel", DataTypes.StringType),
      StructField("resolution", DataTypes.StringType),
      StructField("sessionId", DataTypes.StringType),
      StructField("timeStamp", DataTypes.LongType)
    ))
    val df3 = spark.read.schema(schema2).json("data/app_log_2021-06-07.log")
    df3.printSchema()
    df3.show(50,false)

    df3.selectExpr("properties['couponId']")

    spark.close()
  }
}
