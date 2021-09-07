package cn.doitedu.sparksql.demos

import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession, TypedColumn}


object C18_自定义聚合函数UDAF {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("自定义聚合函数")
      .master("local")
      .getOrCreate()

    val df: DataFrame = spark.read.option("header", "true").csv("data/battel2.txt")
    val df2 = df.selectExpr("id", "name", "role", "cast(energy as double) as energy")

    import spark.implicits._
    /*
    import org.apache.spark.sql.functions._
    df.select($"id",$"name",$"role",$"energy".cast(DataTypes.DoubleType))*/

    df2.createTempView("df2")

    // 注册弱类型的UDAF函数
    spark.udf.register("myavg", MyAverage)

    // 使用函数
    spark.sql(
      """
        |
        |select
        |  role,
        |  myavg3(energy)
        |from df2
        |group by role
        |
        |""".stripMargin).show(100, false)


    /**
     * 在spark2.x中，强类型的Aggregator还没有跟UserDefinedAggregationFunction统一
     * 所以，需要用dataset的DSL风格api来使用
     */
    val ds = df2.as[Soldier]
    val myavg2: TypedColumn[Double, Double] = MyAverage2.toColumn
    ds.groupBy('role).agg(myavg2('energy))

    spark.close()
  }
}


/**
 * 弱类型UDAF接口实现
 *
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-09-07
 * @desc
 */
object MyAverage extends UserDefinedAggregateFunction {
  // 函数的输入参数的结构（几个参数，分别什么类型）
  override def inputSchema: StructType = StructType(Seq(StructField("f1", DataTypes.DoubleType)))

  // 内部运算所需要的缓存结构 ：  定一个数组类型的缓存结构
  override def bufferSchema: StructType = StructType(Seq(
    StructField("s", DataTypes.DoubleType), // 缓存求和值
    StructField("c", DataTypes.IntegerType) // 缓存计数器
  ))

  // 自定义UDAF的最终返回值数据类型
  override def dataType: DataType = DataTypes.DoubleType

  // 函数是否是deterministic（给相同的输入，永远得到相同的结果）
  // 与 spark的执行计划优化策略有关联
  override def deterministic: Boolean = true

  // 对buffer结构进行初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, 0.0)
    buffer.update(1, 0)
  }

  /**
   * 逐条计算的逻辑,更新缓存
   *
   * @param buffer 一个task的buffer
   * @param input  一条输入数据
   */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // 从输入行中获取到战斗力
    val energy = input.getAs[Double](0)
    // 更新buffer中的 “和”
    buffer.update(0, buffer.getAs[Double](0) + energy)
    // 更新buffer中的“计数器”
    buffer.update(1, buffer.getAs[Int](1) + 1)
  }

  /**
   * 各个task的局部buffer，合并成一个全局buffer的逻辑
   * 两个buffer合并的逻辑
   *
   * @param buffer1
   * @param buffer2
   */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // 将buffer2中的“和”累加到buffer1中
    buffer1.update(0, buffer1.getAs[Double](0) + buffer2.getAs[Double](0))

    // 将buffer2中的“计数器”累加到buffer1中
    buffer1.update(1, buffer1.getAs[Int](1) + buffer2.getAs[Int](1))

  }

  /**
   * 对合并后的buffer，计算出一个最终的返回值
   *
   * @param buffer
   * @return
   */
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Double](0) / buffer.getAs[Int](1)
  }
}

/**
 * 强类型UDAF接口实现
 *
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-09-07
 * @desc 三个泛型参数： 1。输入数据的类型   2.缓存结构的类型  3.返回值的类型
 */

case class AvgBuffer(val sum: Double, val cnt: Int)

object MyAverage2 extends Aggregator[Double, AvgBuffer, Double] {
  // 创建一个初始的buffer对象
  override def zero: AvgBuffer = AvgBuffer(0, 0)

  // 逐条计算，更新buffer
  override def reduce(b: AvgBuffer, a: Double): AvgBuffer = {
    val sum = b.sum + a
    val cnt = b.cnt + 1
    AvgBuffer(sum, cnt)
  }

  // buffer合并的逻辑
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    val sum = b1.sum + b2.sum
    val cnt = b1.cnt + b2.cnt
    AvgBuffer(sum, cnt)
  }

  // 返回最终结果
  override def finish(reduction: AvgBuffer): Double = reduction.sum/reduction.cnt

  // 指定buffer数据类型对应的encoder
  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  // 指定最终返回值类型对应的encoder
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

