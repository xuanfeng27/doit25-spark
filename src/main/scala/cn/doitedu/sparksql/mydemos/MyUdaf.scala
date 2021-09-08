package cn.doitedu.sparksql.mydemos

import org.apache.spark.sql.{Dataset, Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

import scala.collection.mutable.ArrayBuffer
case class Soldier(id:Int,name:String,role:String,energy:Double)
object MyUdaf {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("udaf").master("local").getOrCreate()
    val schema = StructType(Seq(
      StructField("id",DataTypes.IntegerType),
      StructField("name",DataTypes.StringType),
      StructField("role",DataTypes.StringType),
      StructField("energy",DataTypes.DoubleType)
    ))
    val df = spark.read.option("header", "true").schema(schema).csv("data/battel2.txt")
    import spark.implicits._

    df.createTempView("df")
  /*  spark.sql(
      """
        |select
        |id,name,role,cast(energy as double)
        |from
        |df
        |""".stripMargin)*/


    //注册弱类型自定义
    spark.udf.register("myavg",myavg)
    spark.sql(
      """
        |select
        |role,
        |myavg(energy) as myavg
        |from
        |df
        |group by role
        |""".stripMargin)



    //
    val ds: Dataset[Soldier] = df.as[Soldier] //
    val sAvgCol = strongAvg.toColumn.name("hhh")
    ds.select(sAvgCol).show()
    //ds.groupBy("role").agg(sAvgCol("energy")).show()


    spark.close()
  }
}
//弱类型UDAF接口实现
object myavg extends UserDefinedAggregateFunction {
  def inputSchema: StructType = StructType(Seq(
    StructField("energy",DataTypes.DoubleType)
  ))

  def bufferSchema: StructType =  StructType(Seq(
    StructField("sum",DataTypes.DoubleType),
    StructField("cnt",DataTypes.IntegerType)
  ))

  def dataType: DataType = DataTypes.DoubleType

  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0,0.0)
    buffer.update(1,0)
  }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0,buffer.getAs[Double](0) + input.getAs[Double](0))
    buffer.update(1,buffer.getAs[Int](1)+1)
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0,buffer1.getAs[Double](0)+buffer2.getAs[Double](0))
    buffer1.update(1,buffer1.getAs[Int](1)+buffer2.getAs[Int](1))
  }

  def evaluate(buffer: Row): Any = buffer.getAs[Double](0)/buffer.getAs[Int](1)
}
// * 强类型UDAF接口实现
case class AarrayBuf(sum:Double,count:Int)
object strongAvg extends Aggregator[Double,AarrayBuf,Double]{
  def zero: AarrayBuf = AarrayBuf(0.0,0)

  def reduce(b: AarrayBuf, a: Double): AarrayBuf = {
    AarrayBuf( b.sum+a,  b.count+1)
  }

  def merge(b1: AarrayBuf, b2: AarrayBuf): AarrayBuf = {
    AarrayBuf(b1.sum+b2.sum,b1.count+b2.count)
  }

  def finish(reduction: AarrayBuf): Double = reduction.sum/reduction.count

  def bufferEncoder: Encoder[AarrayBuf] = Encoders.product

  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}