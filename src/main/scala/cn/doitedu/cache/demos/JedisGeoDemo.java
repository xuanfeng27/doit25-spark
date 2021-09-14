package cn.doitedu.cache.demos;

import redis.clients.jedis.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JedisGeoDemo {
    public static void main(String[] args) {

        // 连接池
        JedisPool jedisPool = new JedisPool("doit01", 6379);
        // 从连接池中取出一个连接
        Jedis jedis = jedisPool.getResource();

        // 插入一个地理位置数据
        jedis.geoadd("area:info",120.30311934861633,31.578424126579673,"JS:WX:LIANGXI");

        // 查询指定地点的坐标
        List<GeoCoordinate> geopos = jedis.geopos("area:info", "JS:WX:LIANGXI");
        System.out.println(geopos.get(0));


        // 查询指定两地的距离
        Double geodist = jedis.geodist("area:info", "JS:WX:XISHAN", "JS:WX:BINHU",GeoUnit.KM);
        System.out.println("JS:WX:XISHAN 和 JS:WX:BINHU 相距： " + geodist);

        // 查询指定坐标附近1km的相邻点
        List<GeoRadiusResponse> georadius = jedis.georadius("area:info", 120.49103, 31.615577, 1, GeoUnit.KM);
        for (GeoRadiusResponse geoRadiusResponse : georadius) {
            System.out.println(geoRadiusResponse.getCoordinate());
            System.out.println(geoRadiusResponse.getMemberByString());
            System.out.println(geoRadiusResponse.getDistance());
        }



        jedis.close();

    }


}
