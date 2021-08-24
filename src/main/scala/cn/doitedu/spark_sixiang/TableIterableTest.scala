package cn.doitedu.spark_sixiang

object TableIterableTest {
  def main(args: Array[String]): Unit = {

    val rdd = new TableIterable("jdbc:mysql://localhost:3306/abc", "battel", "root", "123456")

    val res1 = rdd
      .groupBy(tp=>tp._3)
      .mapValues(iter=>iter.map(_._4).sum)
    println(res1)


    val res2 = rdd
      .groupBy(_._3)
      .mapValues(iter=>iter.map(_._4).max)
    println(res2)


    val res3 = rdd
      .groupBy(_._3)
      .mapValues(iter=>iter.maxBy(tp=>tp._4))

    println(res3)

  }

}
