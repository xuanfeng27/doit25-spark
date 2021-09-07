package cn.doitedu.sparksql.demos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, SparkPlanner}


/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-09-07
 * @desc
 *
 * 一条sql是如何变成可执行的rdd的
 *
 */
object C20_SPARKSQL源码分析 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)

    val spark: SparkSession = SparkSession.builder()
      .appName("源码分析")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    val df1 = spark.createDataset(Seq(
      (1, "zs"),
      (2, "ls")
    )).toDF("id", "name")
    df1.createTempView("tmp_stu")


    val df2 = spark.createDataset(Seq(
      (11, "aa"),
      (22, "bb"),
      (33, "cc")
    )).toDF("id", "name")
    df2.createGlobalTempView("tmp_stu")

    /*spark.sql(
      """
        |
        |select
        |  *
        |from stu
        |""".stripMargin).show(100, false)*/


    // sparksql中，对于库、表、字段等元数据的管理是通过sessionState下的sessionCatalog管理的
    val catalog = spark.sessionState.catalog
    //println(catalog.listTables("default"))
    //println(catalog.listTables("global_temp"))
    //catalog.listFunctions("default")


    // df的执行计划生成入口
    df1.queryExecution

    // sparksession中持有一个 sql解析器
    val sqlParser: ParserInterface = spark.sessionState.sqlParser

    val sqlText = """
                     |SELECT
                     |    o1.id, upper(o1.name) as name,  o2.age + 10+20  as age, o2.addr, o2.phone
                     |FROM
                     |(
                     |SELECT
                     |    id,name,gender,salary
                     |FROM stu
                     |) o1
                     |
                     |JOIN
                     |
                     |(
                     |SELECT
                     |    id,addr,phone,age
                     |FROM info
                     |) o2
                     |
                     |ON o1.id=o2.id
                     |WHERE o1.id>4
                     |
                     |""".stripMargin


    // 未绑定的逻辑执行计划
    val unresolvedLogicPlan = sqlParser.parsePlan(sqlText)
    println(unresolvedLogicPlan)


    val df: DataFrame = spark.sql(sqlText)


    // sparksession中持有一个 分析器 ，用来给未绑定元数据的逻辑执行计划进行元数据绑定
    val analyzer: Analyzer = spark.sessionState.analyzer
    val resolvedLogicPlan: LogicalPlan = analyzer.execute(unresolvedLogicPlan)
    println(resolvedLogicPlan)



    // sparksession中持有一个  优化器
    val optimizer: Optimizer = spark.sessionState.optimizer
    println(df.queryExecution.optimizedPlan)


    // 生成物理执行计划的器
    val sparkPlanner: SparkPlanner = spark.sessionState.planner
    println(df.queryExecution.sparkPlan)


    // 触发物理计划的执行( “行动算子” )
    df.show(10,false)




    spark.close()
  }
}
