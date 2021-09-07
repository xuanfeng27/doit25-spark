package cn.doitedu.sparksql.demos

import cn.doitedu.sparksql.demos.BmUtil.{deserBitMap, serBitMap}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.roaringbitmap.RoaringBitmap

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

object C19_自定义聚合函数实战案例 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("自定义聚合函数上卷案例")
      .master("local")
      .getOrCreate()

    val df: DataFrame = spark.read.option("header", "true").csv("data/bitmap.data.txt")
    val df2 = df.selectExpr("province", "city", "region", "cast(userid as int) as userid")

    df2.createTempView("df2")
    spark.udf.register("get_bitmap",GetBitMap)

    // 获取bitmap的基数
    val bm_cardinality = (bytes:Array[Byte])=>deserBitMap(bytes).getCardinality
    spark.udf.register("get_bm_card",bm_cardinality)

    // 计算省、市、区的用户bitmap
    val proviceAndCityAndRegion = spark.sql(
      """
        |
        |select
        | province,
        | city,
        | region,
        | get_bitmap(userid) as userid_bitmap,
        | get_bm_card(get_bitmap(userid)) as user_cnt
        |
        |from df2
        |group by province,city,region
        |
        |""".stripMargin)
    proviceAndCityAndRegion.show(100,false)


    // 计算省的用户bitmap
    proviceAndCityAndRegion.createTempView("p_c_r")
    spark.udf.register("bitmap_combine",GetBitMap2)
    val province = spark.sql(
      """
        |
        |select
        |  province,
        |  bitmap_combine(userid_bitmap) as userid_bitmap,
        |  get_bm_card(bitmap_combine(userid_bitmap)) as user_cnt
        |
        |from p_c_r
        |group by province
        |
        |""".stripMargin)
    province.show(100,false)


    spark.close()
  }

}

// 接收一组整数聚合成bitmap
object GetBitMap extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(Seq(StructField("userid", DataTypes.IntegerType)))

  // 缓存用的是一个bitmap的序列化字节
  override def bufferSchema: StructType = StructType(Seq(StructField("bitmap_buffer", DataTypes.BinaryType)))


  override def dataType: DataType = DataTypes.BinaryType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // 构造一个空的bitmap
    val bm: RoaringBitmap = RoaringBitmap.bitmapOf()

    // 将它序列化
    val bytes = serBitMap(bm)

    // 更新到缓存
    buffer.update(0,bytes)

  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // 从buffer中取出序列化后的bitmap字节数组
    val bytes: Array[Byte] = buffer.getAs[Array[Byte]](0)

    // 反序列化
    val bm: RoaringBitmap = deserBitMap(bytes)

    // 往bitmap中添加本次输入数据
    bm.add(input.getInt(0))

    // 将bitmap序列化，并更新到缓存
    buffer.update(0,serBitMap(bm))

  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

    val bytes1: Array[Byte] = buffer1.getAs[Array[Byte]](0)
    val bytes2: Array[Byte] = buffer2.getAs[Array[Byte]](0)

    // 反序列化两个bitmap
    val bm1 = deserBitMap(bytes1)
    val bm2 = deserBitMap(bytes2)

    // 合并两个bitmap
    bm1.or(bm2)

    // 序列化合并后的bitmap，并更新到buffer1
    buffer1.update(0,serBitMap(bm1))
  }

  override def evaluate(buffer: Row): Any = buffer.getAs[Array[Byte]](0)



}

// 接收一组bitmap进行聚合
object GetBitMap2 extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(Seq(StructField("bitmap", DataTypes.BinaryType)))

  // 缓存用的是一个bitmap的序列化字节
  override def bufferSchema: StructType = StructType(Seq(StructField("bitmap_buffer", DataTypes.BinaryType)))


  override def dataType: DataType = DataTypes.BinaryType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // 构造一个空的bitmap
    val bm: RoaringBitmap = RoaringBitmap.bitmapOf()

    // 将它序列化
    val bytes = serBitMap(bm)

    // 更新到缓存
    buffer.update(0,bytes)

  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // 从 buffer 中取出序列化后的bitmap字节数组
    val bytes1: Array[Byte] = buffer.getAs[Array[Byte]](0)

    // 从input中取出bitmap的序列化字节
    val bytes2: Array[Byte] = input.getAs[Array[Byte]](0)

    // 反序列化
    val bm1: RoaringBitmap = deserBitMap(bytes1)
    val bm2: RoaringBitmap = deserBitMap(bytes2)

    // 合并两个bitmap
    bm1.or(bm2)

    // 将合并好的bitmap序列化，并更新到缓存
    buffer.update(0,serBitMap(bm1))
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    update(buffer1,buffer2)

  }

  override def evaluate(buffer: Row): Any = buffer.getAs[Array[Byte]](0)

}



object BmUtil{

  def serBitMap(bm:RoaringBitmap):Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val dout = new DataOutputStream(out)
    bm.serialize(dout)

    out.toByteArray
  }

  def deserBitMap(bytes:Array[Byte]):RoaringBitmap = {
    val bm: RoaringBitmap = RoaringBitmap.bitmapOf()
    val baIn = new ByteArrayInputStream(bytes)
    val dataIn = new DataInputStream(baIn)

    // 将字节反序列化到bm对象中
    bm.deserialize(dataIn)

    // 返回bitmap
    bm
  }
}