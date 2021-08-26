package cn.doitedu.spark_sixiang

object FileIteratorTest {
  def main(args: Array[String]): Unit = {

    val iter: Iterator[String] = new FileIterator("data/battel.txt",0,134217728)

    iter.map(s => {
      val arr: Array[String] = s.split(",")
      (arr(0), arr(1), arr(2), arr(3))
    })

  }

}
