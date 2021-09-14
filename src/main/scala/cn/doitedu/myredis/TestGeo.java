package cn.doitedu.myredis;

import redis.clients.jedis.GeoRadiusResponse;
import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;

/**
 * @ClassName: TestGeo
 * @Author: zll
 * @CreateTime: 2021/9/14 20:55
 * @Desc: java 程序
 * @Version: 1.0
 */
public class TestGeo {
    public static void main(String[] args) {
        JedisPool jedisPool = new JedisPool("doit", 6379);
        Jedis jedis = jedisPool.getResource();
        List<GeoRadiusResponse> georadius =
                jedis.georadius("geo:info", 120.49103, 31.615577, 20, GeoUnit.KM);

        for (GeoRadiusResponse response : georadius) {
            System.out.println(response.getDistance());
            System.out.println(response.getMemberByString());
        }




        jedis.close();
    }
}
