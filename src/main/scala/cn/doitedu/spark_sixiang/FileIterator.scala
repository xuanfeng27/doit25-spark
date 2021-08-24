package cn.doitedu.spark_sixiang

import java.io.{BufferedReader, FileReader}

class FileIterator(val path: String) extends Iterator[String] {

  private val br = new BufferedReader(new FileReader(path))

  var line: String = null

  override def hasNext: Boolean = {
    line = br.readLine()
    line != null
  }

  override def next(): String = line
}

class FileIterable(val path:String) extends Iterable[String] {
  override def iterator: Iterator[String] = new FileIterator(path)
}



