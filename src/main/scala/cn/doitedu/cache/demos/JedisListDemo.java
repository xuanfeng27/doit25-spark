package cn.doitedu.cache.demos;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;

public class JedisListDemo {
    public static void main(String[] args) {

        // 连接池
        JedisPool jedisPool = new JedisPool("doit01", 6379);
        // 从连接池中取出一个连接
        Jedis jedis = jedisPool.getResource();


        // 插入一个list到redis中
        Long lpush = jedis.lpush("teachers", "行哥", "星哥", "源哥", "娜姐", "娜妹");

        // 从一个list中获取元素
        List<String> teachers = jedis.lrange("teachers", 0, 1);
        System.out.println(teachers);

        // 从list中弹出元素
        String teachers1 = jedis.lpop("teachers");
        String teachers2 = jedis.rpop("teachers");


        jedis.close();

    }


}
