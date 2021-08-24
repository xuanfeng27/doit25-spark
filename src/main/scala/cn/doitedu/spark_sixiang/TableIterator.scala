package cn.doitedu.spark_sixiang

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

class TableIterator(val url:String ,
                    val tableName:String,
                    val userName:String,
                    val password:String
                   ) extends Iterator[(Int,String,String,Int)]{

  private val conn: Connection = DriverManager.getConnection(url,userName,password)
  private val stmt: PreparedStatement = conn.prepareStatement("select id,name,role,battel from battel")
  private val rs: ResultSet = stmt.executeQuery()

  override def hasNext: Boolean = rs.next()

  override def next(): (Int, String, String, Int) = {
    val id: Int = rs.getInt(1)
    val name: String = rs.getString(2)
    val role: String = rs.getString(3)
    val battel: Int = rs.getInt(4)
    (id,name,role,battel)
  }
}

class TableIterable(val url:String ,
                    val tableName:String,
                    val userName:String,
                    val password:String
                   ) extends Iterable[(Int,String,String,Int)] {
  override def iterator: Iterator[(Int, String, String, Int)] = new TableIterator(url,tableName,userName,password)
}