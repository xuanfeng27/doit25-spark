package cn.doitedu.spark.demos

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import redis.clients.jedis.{GeoRadiusResponse, GeoUnit, Jedis}

import java.{lang, util}

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-09-14
 * @desc 用redis来实现日志数据的地理位置集成
 */
object C16_地理位置集成5 {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("地理位置集成")
      .master("local")
      .getOrCreate()

    import spark.implicits._



    // 读取日志数据
    val ds: Dataset[String] = spark.read.textFile("data/app_log_2021-06-07.log")
    val res: RDD[String] = ds.rdd.mapPartitions(iter =>{

      val jedis = new Jedis("doit01", 6379)

      iter.map(line=>{
        val jsonObject: JSONObject = JSON.parseObject(line)
        // 取出经纬度
        val lng: lang.Double = jsonObject.getDouble("longitude")
        val lat: lang.Double = jsonObject.getDouble("latitude")


        var province = "未知";
        var city = "未知";
        var region = "未知";


        // 查询 redis
        val responses: util.List[GeoRadiusResponse] = jedis.georadius("area:info", lng, lat, 5, GeoUnit.KM)
        if(responses !=null && responses.size()>0){
          val response: GeoRadiusResponse = responses.get(0)
          // 省:市:区
          val areaInfo: String = response.getMemberByString
          val arr: Array[String] = areaInfo.split(":")
          province = arr(0)
          city = arr(1)
          region = arr(2)
        }


        // 组装结果
        jsonObject.put("province",province)
        jsonObject.put("city",city)
        jsonObject.put("region",region)

        // 返回json串结果
        jsonObject.toJSONString

      })
    })


    // 输出保存结果
    res.saveAsTextFile("dataout/redis_area")


    spark.close()
  }
}
