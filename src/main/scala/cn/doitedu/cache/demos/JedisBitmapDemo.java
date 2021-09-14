package cn.doitedu.cache.demos;

import redis.clients.jedis.*;

import java.util.List;

public class JedisBitmapDemo {
    public static void main(String[] args) {

        // 连接池
        JedisPool jedisPool = new JedisPool("doit01", 6379);
        // 从连接池中取出一个连接
        Jedis jedis = jedisPool.getResource();


        // 生成一条bitmap数据，并插入元素  offset: bit偏移量位置
        jedis.setbit("b1",0,"1");
        jedis.setbit("b1",2,"0");
        jedis.setbit("b1",4,"1");
        jedis.setbit("b1",5,"0");
        jedis.setbit("b1",9,"1");
        jedis.setbit("b1",29,"1");

        // start | end是字节偏移量位置
        System.out.println(jedis.bitcount("b1", 0, 1));


        // 生成一条bitmap数据，并插入元素
        jedis.setbit("b2",0,"1");
        jedis.setbit("b2",2,"1");
        jedis.setbit("b2",4,"1");
        jedis.setbit("b2",5,"0");


        // 查询bitmap中的1的个数（基数）
        Long b1Count = jedis.bitcount("b1");
        System.out.println(b1Count);


        // 查询bitmap中的1的个数（基数）
        Long b2Count = jedis.bitcount("b2");
        System.out.println(b2Count);


        // 对两个bitmap操作
        jedis.bitop(BitOP.AND, "r1","b1","b2");
        jedis.bitop(BitOP.OR, "r2","b1","b2");
        jedis.bitop(BitOP.XOR, "r3","b1","b2");
        jedis.bitop(BitOP.NOT, "r4","b1");

        jedis.close();

    }


}
