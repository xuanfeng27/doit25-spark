package cn.doitedu.cache.demos;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Pipeline;

public class JedisTransactionDemo {

    public static void main(String[] args) throws InterruptedException {
        // 连接池
        JedisPool jedisPool = new JedisPool("doit01", 6379);
        // 从连接池中取出一个连接
        Jedis jedis = jedisPool.getResource();

        Pipeline pipelined = jedis.pipelined();

        // 如果需要监视一个key（如果key发生修改则取消事务）
        pipelined.watch("b");  // 如果b的value发生了变化，则下面的事务也就会被自动取消

        // 开启事务
        pipelined.multi();
        pipelined.set("t1","1");
        pipelined.set("t2","2");
        pipelined.set("t3","3");
        pipelined.set("t4","4");
        pipelined.set("t5","5");
        pipelined.incrBy("t2",10);

        // 提交事务
        pipelined.exec();


        jedis.close();
    }
}
