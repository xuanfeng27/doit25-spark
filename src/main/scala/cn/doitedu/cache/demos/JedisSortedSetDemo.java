package cn.doitedu.cache.demos;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class JedisSortedSetDemo {
    public static void main(String[] args) {

        // 连接池
        JedisPool jedisPool = new JedisPool("doit01", 6379);
        // 从连接池中取出一个连接
        Jedis jedis = jedisPool.getResource();


        // 插入一条sortedset数据
        HashMap<String, Double> socreMap = new HashMap<>();
        socreMap.put("彭冯",95.0);
        socreMap.put("董彩月",96.6);
        socreMap.put("张玉茹",97.5);
        socreMap.put("董玉庆",98.5);
        socreMap.put("周建华",60.5);
        socreMap.put("彭智勇",57.5);
        socreMap.put("刘广磊",99.5);

        jedis.zadd("doit25:score",socreMap);

        // 取成绩前3名
        Set<String> zrange = jedis.zrange("doit25:score", 0, 2);
        System.out.println(zrange);

        // 获取张玉茹的降序名次
        Long 名次 = jedis.zrevrank("doit25:score", "张玉茹");
        System.out.println(名次);


        jedis.close();

    }


}
