package cn.doitedu.cache.demos;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.sql.*;

public class 经纬度数据批量入库 {

    public static void main(String[] args) throws SQLException {

        Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/abc", "root", "123456");

        Jedis jedis = new Jedis("doit01", 6379);
        Pipeline pipelined = jedis.pipelined();

        Statement stmt = conn.createStatement();
        ResultSet resultSet = stmt.executeQuery("select province,city,region,lng,lat from area_flat");

        while(resultSet.next()){
            String province = resultSet.getString(1);
            String city = resultSet.getString(2);
            String region = resultSet.getString(3);
            double lng = resultSet.getDouble(4);
            double lat = resultSet.getDouble(5);

            pipelined.geoadd("area:info",lng,lat,province+":"+city+":"+region);
        }

        resultSet.close();
        stmt.close();
        conn.close();


        pipelined.sync();
        jedis.close();


    }

}
