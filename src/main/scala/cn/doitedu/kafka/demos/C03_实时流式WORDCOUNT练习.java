package cn.doitedu.kafka.demos;

import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import scala.collection.Seq;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class C03_实时流式WORDCOUNT练习 {

    // kafka中，有源源不断的新数据：一个一个的单词

    //  TODO 每5秒统计一次，截止到当前，每个单词的出现次数
    {
        HashMap<String, Integer> map = new HashMap<>();
        map.put("word",map.getOrDefault("word",0)+1);
        HashMap<String, Integer> map2 = new HashMap<>();
        map2.put("word",map2.getOrDefault("word",0)+1);
        Set<String> keys = map.keySet();
        HashMap<String, Integer> map3 = new HashMap<>();
        for (String key : keys) {
            map3.put(
                    key,
                    map.getOrDefault(key,0)+map2.getOrDefault(key,0)
            );
        }
    }

    //  TODO 每5秒统计一次，最近10秒钟内的数据的wordcount

}
