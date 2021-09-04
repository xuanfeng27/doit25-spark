package cn.doitedu.sparksql.demos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, DatasetHolder, Encoders, Row, SparkSession}

case class Grade(level:Int,stuCount:Int,teacher:String)

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-09-04
 * @desc
 *    一个RDD[T] 转成dataframe，有两种方式：
 *        存在对应的T类型的隐式Encoder的，直接 rdd.toDF()
 *        不存在对应T类型的隐式Encoder的，用  spark.createDataframe(beanRdd,beanClass)
 *                                       spark.createDataframe(rowRdd,schema)
 */
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


    /**
     * 四、将 scala Bean 的 rdd转成 dataframe
     * 注意： scalaBean中的属性需要加上 @BeanProperty 注解
     */
    println("============== df4 ================")
    val rdd4: RDD[ScalaGrade] = sc.makeRDD(Seq(
      new ScalaGrade(1,50,"刘坤"),
      new ScalaGrade(2,55,"柳坤"),
      new ScalaGrade(3,60,"刘昆"),
      new ScalaGrade(4,40,"坤坤")
    ))
    val df4: DataFrame = spark.createDataFrame(rdd4, classOf[ScalaGrade])
    df4.printSchema()
    df4.show(100,false)
    // rdd4.map(bean=>(bean.level,bean.stuCount,bean.teacher)).toDF("level","stuCount","teacher")


    /**
     * 五、将 Row 的 rdd转成 dataframe
     *
     */
    println("============== df5 ================")
    val rdd5 = sc.makeRDD(Seq(
      Row(1,"zs",18),
      Row(1,"zs",18),
      Row(1,"zs",18),
      Row(1,"zs",18),
      Row(1,"zs",18)
    ))
    val schema = StructType(Seq(
      StructField("id",DataTypes.IntegerType),
      StructField("name",DataTypes.StringType),
      StructField("age",DataTypes.IntegerType),
    ))
    val df5: DataFrame = spark.createDataFrame(rdd5, schema)



    spark.close()

  }

}
