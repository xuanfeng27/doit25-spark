package cn.doitedu.spark_sixiang

import java.io.{BufferedReader, FileReader}

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-08-24
 * @desc 自定义rdd的总抽象接口
 */
trait Rdd {
  var dep: List[Rdd]
  val iter: Iterator[String]
  def map(f: String => String): MapRdd
  def compute(): Iterator[String]
  def foreach(f:String=>Unit):Unit
}


/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-08-24
 * @desc 模拟从seq内存集合创建的RDD实现类
 */
class SeqRdd(val c: Seq[String]) extends Rdd {
  override val iter = compute()
  override var dep: List[Rdd] = Nil

  override def compute(): Iterator[String] = {
    c.iterator
  }

  override def map(f: String => String): MapRdd = {
    new MapRdd(iter => iter.map(f), List(this))
  }

  override def foreach(f: String => Unit): Unit = iter.foreach(f)
}

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-08-24
 * @desc 模拟从文件加载数据得到的rdd的实现类
 */
class FileRdd(val path: String) extends Rdd {
  override val iter = compute()
  override var dep: List[Rdd] = Nil

  override def compute(): Iterator[String] = {
    new FileIterator(path)
  }

  override def map(f: String => String): MapRdd = {
    new MapRdd(iter => iter.map(f), List(this))
  }

  override def foreach(f: String => Unit): Unit = iter.foreach(f)

  class FileIterator(val s: String) extends Iterator[String] {
    private val br = new BufferedReader(new FileReader(s))
    private var line: String = _
    private var flag:Boolean = false

    override def hasNext: Boolean = {
      line = br.readLine()
      if(line != null) flag = true else flag=false
      flag
    }

    override def next(): String = {
      line
    }
  }
}


/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-08-24
 * @desc 模拟从数据源rdd经过map计算后得到的中间层RDD的实现类
 */
class MapRdd(val f: Iterator[String] => Iterator[String],
             var dep: List[Rdd]) extends Rdd {
  def compute(): Iterator[String] = {
    f(dep(0).iter)
  }

  def map(f: String => String): MapRdd = {
    new MapRdd(iter => iter.map(f), List(this))
  }

  override val iter: Iterator[String] = compute()

  override def foreach(f: String => Unit): Unit = iter.foreach(f)
}

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-08-24
 * @desc 模拟spark的context
 */
class Context{
  def textFile(path:String):FileRdd = {
    new FileRdd(path)
  }
  def makeRdd(seq:Seq[String]):SeqRdd = {
    new SeqRdd(seq)
  }

  def runJob(rdd:Rdd): Unit ={
    val iter = rdd.iter
    iter.foreach(println)
  }
}

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-08-24
 * @desc 测试代码
 */
object Test {
  def main(args: Array[String]): Unit = {
    val sc = new Context
    val rdd = sc.textFile("d:/a.txt")
    val res = rdd.map(_.toUpperCase())
      .map("xx - " + _)
      .map(_ ++ " _ ")

    res.foreach(println)

  }
}