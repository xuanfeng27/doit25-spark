package cn.doitedu.cache.demos;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

public class JedisPubSubDemo {

    public static void main(String[] args) throws InterruptedException {

        new Thread(new Runnable() {
            @Override
            public void run() {
                // 连接池
                JedisPool jedisPool = new JedisPool("doit01", 6379);
                // 从连接池中取出一个连接
                Jedis jedis = jedisPool.getResource();

                // 订阅channel(频道）
                jedis.subscribe(new JedisPubSub() {
                    @Override
                    public void onMessage(String channel, String message) {
                        System.out.println(channel + " -> " + message);
                    }
                }, "ch1","ch2");
            }
        }).start();



        // 连接池
        JedisPool jedisPool = new JedisPool("doit01", 6379);
        // 从连接池中取出一个连接
        Jedis jedis = jedisPool.getResource();
        // 发布消息
        while(true){
            jedis.publish("ch1", RandomStringUtils.randomAlphabetic(3,6));
            Thread.sleep(RandomUtils.nextInt(200,2000));
        }

    }
}
