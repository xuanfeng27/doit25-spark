package cn.doitedu.spark.demos

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-08-27
 * @desc
 * 2021-08-06号的连续活跃区间记录表
D00001	2021-08-01	2021-08-04
D00001	2021-08-06	9999-12-31
D00002	2021-08-03	2021-08-03
D00002	2021-08-05	2021-08-05
D00003	2021-08-01	2021-08-03
D00003	2021-08-05	9999-12-31


2021-08-07号的日志
D00003
D00002
D00005


==》 先挑出那些已经封闭的区间记录
D00001	2021-08-01	2021-08-04
D00002	2021-08-03	2021-08-03
D00002	2021-08-05	2021-08-05
D00003	2021-08-01	2021-08-03

==》 再挑出区间未封闭的记录   FULL JOIN
D00001	2021-08-06	9999-12-31       None
D00003	2021-08-05	9999-12-31       D00003
                     None            D00002
                     None            D00005


合并最终结果：
D00001	2021-08-01	2021-08-01	2021-08-04
D00001	2021-08-01	2021-08-06	2021-08-06
D00002	2021-08-03	2021-08-03	2021-08-03
D00002	2021-08-03	2021-08-05	2021-08-05
D00002	2021-08-03	2021-08-07	9999-12-31
D00003	2021-08-01	2021-08-01	2021-08-03
D00003	2021-08-01	2021-08-05	9999-12-31
D00005  2021-08-07  2021-08-07  9999-12-31

 */

object C06_综合练习_连续活跃区间 {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("连续活跃区间")
    val sc = new SparkContext(conf)

    // 从2021-06-07日志文件创建RDD
    val logRdd: RDD[String] = sc.textFile("data/app_log_2021-06-07.log")
    // 对日志数据做json解析，提取里面的deviceid
    val deviceIdRdd = logRdd.map(line=>{
      val jSONObject: JSONObject = JSON.parseObject(line)
      jSONObject.getString("deviceId")
    })
    // 对deviceid去重，得到今天的日活用户id
    val distincedDeviceids: RDD[String] = deviceIdRdd.distinct(4)


    // 从2021-06-06连续活跃区间表文件创建RDD
    val continueActiveRdd: RDD[String] = sc.emptyRDD[String]
    // D00001,2021-08-01,2021-08-04
    val continueActiveTuples = continueActiveRdd.map(line=>{
      val arr: Array[String] = line.split(",")
      (arr(0),arr(1),arr(2))
    })


    // 挑选已经封闭区间的记录
    val closedRecords: RDD[(String, String, String)] = continueActiveTuples.filter(tp => !tp._3.equals("9999-12-31"))

    // 挑选未封闭区间的记录
    val unClosedRecords  = continueActiveTuples.filter(tp => tp._3.equals("9999-12-31"))


    // 将待join的两个rdd都变成KV结构
    val kvUnClosedRecords = unClosedRecords.map(tp=>(tp._1,tp))
    val kvDistincedDeviceIds = distincedDeviceids.map(id=>(id,id))

    /**
     *       未封闭的记录   FULL JOIN    去重的日活deviceId
     *       D00001	2021-08-06	9999-12-31         None
     *       D00003	2021-08-05	9999-12-31        D00003
     *                            None            D00002
     *                            None            D00005
     */
    val joined: RDD[(String, (Option[(String, String, String)], Option[String]))] = kvUnClosedRecords.fullOuterJoin(kvDistincedDeviceIds)
    // 按照事先想好的逻辑进行判断，取数
    val resOptionPart1: RDD[Option[(String, String, String)]] = joined.map(tp=>{
      val twoTable: (Option[(String, String, String)], Option[String]) = tp._2
      val data: Option[(String,String,String)] = twoTable match {
        case (Some((deviceId,startDt,endDt)),None) => Some((deviceId,startDt,"2021-06-07"))
        case (Some((deviceId,startDt,endDt)),Some(_)) => Some((deviceId,startDt,endDt))
        case (None,Some(deviceId)) => Some((deviceId,"2021-06-07","9999-12-31"))
        case _ => None
      }
      data
    })


    // 过滤掉一些可能的异常数据，从Option解开为三元组
    val resPart1 = resOptionPart1.filter(opt=>opt.isDefined).map(opt=>opt.get)


    // 将结果part1 和  已封闭的区间记录数据  合并
    val result: RDD[(String, String, String)] = closedRecords.union(resPart1)


    result.map(tp=>tp._1 + "," + tp._2 + "," + tp._3).saveAsTextFile("dataout/continue_out/2021-06-07/")

    sc.stop()
  }
}
