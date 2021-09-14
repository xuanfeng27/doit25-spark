package cn.doitedu.cache.demos;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class JedisSetDemo {
    public static void main(String[] args) {

        // 连接池
        JedisPool jedisPool = new JedisPool("doit01", 6379);
        // 从连接池中取出一个连接
        Jedis jedis = jedisPool.getResource();

        // 插入一个set集合数据
        jedis.sadd("liyang:girlfriends","金艳","金艳","金艳","金艳");

        // 获取一个set集合数据
        Set<String> smembers = jedis.smembers("liyang:girlfriends");

        // 判断一个元素是否存在于指定的set集合中
        boolean isMember = jedis.sismember("liyang:girlfriends", "张天爱");

        System.out.println(isMember);


        jedis.close();

    }


}
