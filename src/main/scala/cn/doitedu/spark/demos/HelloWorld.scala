package cn.doitedu.spark.demos

object HelloWorld {
  def main(args: Array[String]): Unit = {

    val lst = List(1, 2, 3, 4)
    lst.map(x=>{
      println("调用了函数")
      x+10
    })


  }
}
