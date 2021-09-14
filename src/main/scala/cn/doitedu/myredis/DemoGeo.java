package cn.doitedu.myredis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.sql.*;

/**
 * @ClassName: DemoGeo
 * @Author: zll
 * @CreateTime: 2021/9/14 20:34
 * @Desc: java 程序
 * @Version: 1.0
 */
public class DemoGeo {
    public static void main(String[] args) throws SQLException {
        Jedis jedis = new Jedis("doit", 6379);
        Pipeline pipelined = jedis.pipelined();

        Connection conn = DriverManager.getConnection(
                "jdbc:mysql://localhost:3306/day01",
                "root",
                "daydayup"
        );
        PreparedStatement statement = conn.prepareStatement("select province,city,area,BD09_LAT,BD09_LNG from area_city_province");
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()){
            String province = resultSet.getString(1);
            String city = resultSet.getString(2);
            String area = resultSet.getString(3);
            double lat = resultSet.getDouble(4);
            double lng = resultSet.getDouble(5);

            Response<Long> response =
                    pipelined.geoadd("geo:info", lng, lat, province + "," + city + "," + area);
        }



        resultSet.close();
        conn.close();

        pipelined.sync();
        jedis.close();
    }
}
