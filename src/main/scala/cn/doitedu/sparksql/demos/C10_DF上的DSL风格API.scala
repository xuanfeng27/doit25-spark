package cn.doitedu.sparksql.demos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import java.util.Properties


/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-09-05
 * @desc DSL风格API演示
 */
object C10_DF上的DSL风格API {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark: SparkSession = SparkSession.builder()
      .appName("C10_DF上的DSL风格API")
      .config("spark.sql.crossJoin.enabled","true")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

    val schema = StructType(Seq(
      StructField("id", DataTypes.IntegerType),
      StructField("name", DataTypes.StringType),
      StructField("role", DataTypes.StringType),
      StructField("energy", DataTypes.DoubleType)
    ))


    val df = spark.read.schema(schema).options(Map("header" -> "true")).csv("data/battel2.txt")


    /**
     * 一、 SELECT 操作
     */
    // select id,name,role from df
    df.select("id", "name", "role")

    // df("字段名") 也能转成一个column对象
    val idColumn: Column = df("id")
    df.select(idColumn, df("name"), df("role"))

    // $"字段名" 也能转成一个column对象
    import spark.implicits._
    val idC: Column = $"id"
    df.select(idC, $"name", $"role")

    // 单引号写一边，也能转成一个Column对象
    df.select('id, 'name, 'role)

    // 利用api函数  col ，也能根据一个字段名得到一个字段对象
    import org.apache.spark.sql.functions._

    val column1: Column = col("id")
    df.select(column1, col("name"), column("role"))


    // select id,upper(name),role from df
    // df.select("id","upper(name)","role").show()  //会报错，select会把upper(name)整体看成一个字段名
    df.selectExpr("id", "upper(name)", "role") //selectExpr会解析函数、运算符等表达式
    df.select('id, upper('name), 'role) // 当然，用select也可以，不过里面需要用Column的形式


    /**
     * 二、 WHERE 操作
     */

    // select * from df where id>3
    df.where("id > 3")
    df.where('id > 3)


    /**
     * 三、 GROUP BY 操作
     */
    // select role,sum(energy),max(energy) from df group by role
    df.groupBy("role").sum("energy") // 只有一种聚合算子需求时可以这样写

    df.groupBy("role")
      .agg(
        sum('energy) as "energy_sum",
        max('energy) as "energy_max",
        min('id) as "id_min"
      ) // 当有多种聚合算子需求时这样写

    df.groupBy("role")
      .agg(
        Map(
          "energy" -> "sum",
          "energy" -> "max", // key相同会覆盖掉上一条 entry
          "id" -> "min"
        )
      ) // 当有多种聚合算子需求时这样写


    /**
     * 四、 order by 操作
     */
    // select * from df order by energy,id
    df.orderBy("energy", "id")

    // select * from df order by energy desc,id asc
    df.orderBy($"energy" desc, 'id asc)


    /*
     * 五、 窗口函数 操作
     */
    /*
     * select
     *   id,name,role,energy
     * from (
     *   select
     *     id,name,role,energy,row_number() over(partition by role order by energy desc) as rn
     *   from df
     * ) o
     * where rn <= 1
     */
    //用字符串表达式形式写窗口函数
    val res = df.selectExpr("id",
      "name",
      "role",
      "energy",
      "row_number() over(partition by role order by energy desc) as rn"
    )
      .where("rn<=1")
      .drop("rn") // 从表中去掉多个指定字段

    //用纯api形式写窗口函数
    val window: WindowSpec = Window.partitionBy("role").orderBy('energy desc)
    df.select('id, 'name, 'role, 'energy, row_number().over(window) as "rn").
      where("rn<=1")
      .drop("rn")



    /**
     * 六、 JOIN操作
     */
    val props = new Properties()
    props.load(this.getClass.getClassLoader.getResourceAsStream("mysql.properties"))
    val info = spark.read.jdbc(props.getProperty("url"),"battel_info",props)

    // select df.*,info.* from df join info on df.id=info.id
    // 没有join条件，就是产生笛卡尔积 ，需要设置参数 spark.sql.crossJoin.enabled=true;
    df.join(info)
    df.join(info,df("id")===info("id"))  // 用column对象表达  join条件
    df.join(info,"id")     // 用usingColumn表达join条件，要求两表有一个名字相同的列，而且join结果中只保留一个 id列
    df.join(info,Seq("id"),"left")  // cross,inner,left,left_outer,outer,full_outer,right,right_outer,left_semi,left_anti

    /**
     * 七、 UNION操作
     */
    df.union(df).show()




    spark.close()
  }
}
