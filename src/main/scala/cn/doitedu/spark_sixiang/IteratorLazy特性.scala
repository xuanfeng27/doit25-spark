package cn.doitedu.spark_sixiang

object IteratorLazy特性 {

  def main(args: Array[String]): Unit = {

    /*class Person1() {
      def sayHello: String = "hello doitedu"

      def map(f: String => String) = new Person2(f)
    }

    class Person2(f: String => String) {
      def sayHello: String = f((new Person1).sayHello)

      def map(f: String => String) = new Person2(f)
    }


    val iter1 = new Person1()
    val iter2 = iter1.map(s => s.toUpperCase)
    val iter3 = iter2.map(s => s + " haha")

    iter3.sayHello*/




    val lst = List(1, 2, 3, 4, 5)


    val f1 = (x: Int) => {
      println("函数f1被调用了")
      x * 10
    }

    val f2 = (x: Int) => {
      println("函数f2被调用了")
      x + 1000
    }

    // val lst2  = lst.map(f1)
//
    val iter1 = lst.iterator
    val iter2 = iter1.map(f1)
    val iter3 = iter2.map(f2)

    iter3.foreach(println)



  }

}
