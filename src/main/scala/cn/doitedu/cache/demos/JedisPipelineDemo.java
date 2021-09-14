package cn.doitedu.cache.demos;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import java.util.List;
import java.util.Set;

public class JedisPipelineDemo {
    public static void main(String[] args) {

        // 连接池
        JedisPool jedisPool = new JedisPool("doit01", 6379);
        // 从连接池中取出一个连接
        Jedis jedis = jedisPool.getResource();


        Pipeline pipelined = jedis.pipelined();
        pipelined.set("a","10");
        pipelined.set("b","2");
        pipelined.set("c","3");
        pipelined.set("d","4");
        pipelined.set("e","5");
        pipelined.incr("a");
        pipelined.lpush("lst","a","b","c","d");
        pipelined.get("a");
        List<Object> resList = pipelined.syncAndReturnAll();
        System.out.println(resList);


        jedis.close();

    }


}
