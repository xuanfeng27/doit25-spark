package cn.doitedu.sparksql.demos

import org.roaringbitmap.RoaringBitmap

object BitMapDemo {
  def main(args: Array[String]): Unit = {

    val bm1: RoaringBitmap = RoaringBitmap.bitmapOf(1, 3, 5, 6)
    val bm2: RoaringBitmap = RoaringBitmap.bitmapOf(1, 2, 5, 4)

    // 往bitmap中添加元素
    bm1.add(7,8,10)

    // 对两个bitmap按位求 “或”
    bm1.or(bm2)

    // 获取一个bitmap中1的个数
    println(bm1.getCardinality)


  }
}
