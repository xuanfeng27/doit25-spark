package cn.doitedu.spark_sixiang

import scala.reflect.ClassTag

/**
 * rdd总抽象类
 * 带泛型的超真实版本
 * @param classTag$T$0
 * @tparam T
 */
abstract class DoItRdd[T : ClassTag]{

  def compute():Iterator[T]

  val iter:Iterator[T] = compute

  def dependencies_ : Seq[DoItRdd[_]]

  def map[U : ClassTag] (f: T => U ):DoItMapRdd[U,T]

  def foreach(f:T=>Unit):Unit = iter.foreach(f)
}

/**
 * 模拟内存数据的rdd实现类
 * @param seq
 * @param classTag$T$0
 * @tparam T
 */
class CollectionRdd[T : ClassTag](val seq:List[T]) extends DoItRdd[T] {
  override def compute(): Iterator[T] = seq.iterator

  override def dependencies_  = List.empty

  override def map[U :ClassTag](f: T => U): DoItMapRdd[U,T] = new DoItMapRdd[U,T](iter=>iter.map(f),List(this))
}


/**
 * 模拟map运算后生成的中间RDD实现类
 * @param f
 * @param deps
 * @param classTag$T$0
 * @param classTag$U$0
 * @tparam T
 * @tparam U
 */
class DoItMapRdd[T : ClassTag,U:ClassTag](val f:Iterator[U]=>Iterator[T], val deps:List[DoItRdd[U]] ) extends  DoItRdd[T] {
  override def compute(): Iterator[T] = f(deps(0).compute())

  override def dependencies_  = deps

  override def map[U :ClassTag](f: T => U): DoItMapRdd[U, T] = new DoItMapRdd[U,T](iter=>iter.map(f),List(this))
}


/**
 * 测试代码
 */
object TestDoItRdd{
  def main(args: Array[String]): Unit = {

    val rdd = new CollectionRdd[String](List("1", "2", "3"))
    val rdd2: DoItMapRdd[Int, String] = rdd.map(s => s.toInt)
    val res = rdd2.map(s => s * 10).map(s => s.toString).map(s => s + "1").map(s => s.toInt)
    res.foreach(println)

  }
}