package cn.doitedu.cache.demos;

import redis.clients.jedis.BitOP;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-09-14
 * @desc HyperLogLog
 */
public class JedisHLLDemo {
    public static void main(String[] args) {

        // 连接池
        JedisPool jedisPool = new JedisPool("doit01", 6379);
        // 从连接池中取出一个连接
        Jedis jedis = jedisPool.getResource();

        // 生成一个hyperloglog数据，并插入元素
        jedis.pfadd("hll1","aaa","bbb","abc","bcc","bcc","bbc","bbb");

        // 查询基数
        long cnt1 = jedis.pfcount("hll1");
        System.out.println(cnt1);



        // 生成一个hyperloglog数据，并插入元素
        jedis.pfadd("hll2","aac","bbb","abc","bcc","ccc","jjj");

        // 查询基数
        long cnt2 = jedis.pfcount("hll2");
        System.out.println(cnt2);


        // 合并多个hyperloglog数据
        jedis.pfmerge("merge1","hll1","hll2");


        // 查询基数
        long cnt3 = jedis.pfcount("merge1");
        System.out.println(cnt3);




        jedis.close();

    }


}
