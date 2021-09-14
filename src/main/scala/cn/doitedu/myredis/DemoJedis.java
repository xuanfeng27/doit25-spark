package cn.doitedu.myredis;

import redis.clients.jedis.Jedis;

/**
 * @ClassName: DemoJedis
 * @Author: zll
 * @CreateTime: 2021/9/14 20:13
 * @Desc: java 程序
 * @Version: 1.0
 */
public class DemoJedis {
    public static void main(String[] args) {
        Jedis jedis = new Jedis("doit",6379);
        jedis.set("b","222");
        String b = jedis.get("b");
        System.out.println(b);

        jedis.close();
    }
}
