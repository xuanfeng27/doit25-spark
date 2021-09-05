package cn.doitedu.sparksql.mydemos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession, types}

case class Animal(name:String,hobby:String)
object Day01_1 {
  def main(args: Array[String]): Unit = {
    //DataFrame其实就是Dataset[Row]
    Logger.getLogger("org").setLevel(Level.WARN)

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("day01")
      .getOrCreate()


    val schema = StructType(Seq(
      StructField("name", DataTypes.StringType),
      StructField("age", DataTypes.IntegerType)
    ))
    val df: DataFrame = spark.read.schema(schema).json("data/people.dat")
    df.printSchema()
    df.show()
sys.exit(1)
    // 1.提供隐式转换支持，如 RDDs to DataFrames
    import spark.implicits._
    val rdd: RDD[Int] = spark.sparkContext.makeRDD(Seq(1, 2, 3, 4, 5))
    val df1 = rdd.toDF("id")
    df1.printSchema()
    df1.show()
/*  val df0 = spark.createDataFrame(rdd, classOf[StructType])*/



    val animal: RDD[Animal] = spark.sparkContext.makeRDD(Seq(
      Animal("aaa", "a"),
      Animal("bbb", "b"),
      Animal("ccc", "c")
    ))
    val df_a = spark.createDataFrame(animal)
    val df_b = animal.toDF()
    df_a.show()
    df_b.show()


    //
    df.createTempView("people")
    spark.sql(
      """
        |select
        |*
        |from
        |people
        |where age >20
        |""".stripMargin).show()

    //
   // df.write.parquet("data/parq")
   // spark.read.parquet("data/parq").show()



    val df2 = spark.read.json("data/app_log_2021-06-07.log")
    df2.printSchema()
    df2.show(10,false)
    val dataFrame = df2.selectExpr("properties.account")





    spark.close()
  }
}
