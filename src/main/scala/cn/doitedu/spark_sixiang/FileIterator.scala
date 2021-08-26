package cn.doitedu.spark_sixiang

import java.io.{BufferedReader, FileReader, RandomAccessFile}

class FileIterator(val path: String,startOffset:Long,length:Long) extends Iterator[String] {

  private val br = new RandomAccessFile(path, "r")
  br.seek(startOffset)


  var line: String = null
  var alreadReadLength = 0L

  override def hasNext: Boolean = {
    line = br.readLine()
    alreadReadLength += line.length

    alreadReadLength < length && line != null
  }

  override def next(): String = line
}

class FileIterable(val path:String) extends Iterable[String] {
  override def iterator: Iterator[String] = new FileIterator(path,0,134217728)
}



