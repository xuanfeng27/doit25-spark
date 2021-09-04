package cn.doitedu.sparksql.demos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, DatasetHolder, Encoders, SparkSession}

case class Grade(level:Int,stuCount:Int,teacher:String)

object C06_从RDD创建DF {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache").setLevel(Level.WARN)

    val spark: SparkSession = SparkSession.builder()
      .appName("")
      .master("local")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext


    /**
     * 一、将元组rdd转成dataframe
     */
    val rdd: RDD[(Int, String, String, Int)] = sc.makeRDD(Seq((1,"zs","male",18),(2,"aa","female",28),(3,"bb","male",16),(4,"cc","mfeale",24)))
    // 方式1： 手动调用转换方法来将rdd转dataframe
    val df1: DataFrame = spark.createDataFrame(rdd)

    // 方式2： 利用隐式转换工具，直接在rdd上调toDF
    import spark.implicits._
    val df11: DataFrame = rdd.toDF
    // rdd 被 SQLImpliciits中的一个隐式方法转成了DatasetHolder，而DatasetHolder上有toDF方法
    /*val ds: Dataset[(Int, String, String, Int)] = spark.createDataset(rdd)
    val datasetHolder: DatasetHolder[(Int, String, String, Int)] = DatasetHolder[(Int, String, String, Int)](ds)
    val df: DataFrame = datasetHolder.toDF()*/

    df1.printSchema()
    df1.show(100,false)
    df1.where("_3 = 'male' ").select("_1","_2","_4").show(100,false)




    /**
     * 二、将case class 的 rdd转成dataframe
     */
    val rdd2: RDD[Grade] = sc.makeRDD(Seq(
      Grade(1,50,"刘坤"),
      Grade(2,55,"柳坤"),
      Grade(3,60,"刘昆"),
      Grade(4,40,"坤坤")
    ))
    //val df2: DataFrame = spark.createDataFrame(rdd2)
    val df2: DataFrame = rdd2.toDF()  // 利用了隐式转换
    df2.printSchema()
    df2.show(100,false)




    /**
     * 三、将 javaBean 的 rdd转成dataframe
     */
    val rdd3: RDD[JavaGrade] = sc.makeRDD(Seq(
      new JavaGrade(1,50,"刘坤"),
      new JavaGrade(2,55,"柳坤"),
      new JavaGrade(3,60,"刘昆"),
      new JavaGrade(4,40,"坤坤")
    ))
    // RDD转 dataframe，是需要一个Encoder的
    // 只不过前面的case calss和tuple类型，都可以从隐式上下文中自动获取一个对应类型的Encoder
    val df3 = spark.createDataFrame(rdd3,classOf[JavaGrade])
    df3.printSchema()
    df3.show(100,false)








    spark.close()

  }

}
