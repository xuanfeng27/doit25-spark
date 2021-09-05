package cn.doitedu.sparksql.demos

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

case class Teacher(id:Int,name:String,age:Int)

object C14_DF和DS和RDD之间的互转 {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("我恨你")
      .master("local")
      .getOrCreate()


    val rdd = spark.sparkContext.makeRDD(Seq(
      (1,"aa",18),
      (2,"bb",28),
      (3,"cc",38),
      (4,"dd",28),
    ))

    import spark.implicits._

    // rdd 转dataset
    val ds: Dataset[(Int, String, Int)] = rdd.toDS()

    // rdd 转 dataframe
    val df1: DataFrame = rdd.toDF()

    // dataset 转 dataframe
    val df2: DataFrame = ds.toDF()

    // datase 转 rdd
    val rdd2: RDD[(Int, String, Int)] = ds.rdd


    // dataframe 转 dataset  => dataset[Row] 转 Dataset[U]
    val ds2: Dataset[(Int, String, Int)] = df2.as[(Int, String, Int)]
    val ds4 = df1.as[Teacher]

    // dataframe 转 rdd
    val rdd3: RDD[Row] = df2.rdd


  }

}
