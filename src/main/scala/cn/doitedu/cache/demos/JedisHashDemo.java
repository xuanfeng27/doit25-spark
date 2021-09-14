package cn.doitedu.cache.demos;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JedisHashDemo {
    public static void main(String[] args) {

        // 连接池
        JedisPool jedisPool = new JedisPool("doit01", 6379);
        // 从连接池中取出一个连接
        Jedis jedis = jedisPool.getResource();


        // 插入一个hash结构数据到redis中
        HashMap<String, String> mp = new HashMap<>();
        mp.put("name","李建国");
        mp.put("age","28");
        mp.put("gender","男");
        mp.put("salary","28000");

        jedis.hmset("laoli",mp);


        // 取整条hash结构的数据
        Map<String, String> laoli = jedis.hgetAll("laoli");
        System.out.println(laoli);

        // 取一个field
        String salary = jedis.hget("laoli", "salary");
        System.out.println(salary);

        // 取所有的属性名
        Set<String> fieldNames = jedis.hkeys("laoli");
        System.out.println(fieldNames);

        // 删掉一个属性
        jedis.hdel("laoli","age","gender");



        jedis.close();

    }


}
