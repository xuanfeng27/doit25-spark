object Test {
  def main(args: Array[String]): Unit = {

    val iter1: Iterator[Int] = List[Int](1,2,3).iterator
    val iter2: Iterator[Int] = List[Int]().iterator

    for(e1 <- iter1 ){
      println(e1)
    }




  }

}
