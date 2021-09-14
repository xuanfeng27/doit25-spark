package cn.doitedu.cache.demos;

import org.apache.commons.lang3.RandomUtils;

import java.sql.*;
import java.util.HashMap;

public class LocalCacheDemo {

    // 本地缓存
    private static HashMap<Integer, String> cache = new HashMap();

    // 外部缓存访问客户端


    public static void main(String[] args) throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/abc", "root", "123456");

        long start = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            int id = RandomUtils.nextInt(1, 10000);

            String data = getData(conn, id);
            System.out.println(data);
        }

        long end = System.currentTimeMillis();
        System.out.println(end - start);

    }


    public static String getData(Connection conn, int id) throws Exception {

        // 先看一下本地缓存中是否有所需的数据
        String res = cache.get(id);
        if(null != res) return res;

        // 再去外部缓存系统中查询数据





        // 缓存中没有查到，走数据库查询
        PreparedStatement pst = conn.prepareStatement("select id,name,role,battel from battel where id = ?");
        pst.setInt(1, id);
        ResultSet rs = pst.executeQuery();

        String name = "";
        String role = "";
        String battel = "";


        while (rs.next()) {
            name = rs.getString("name");
            role = rs.getString("role");
            battel = rs.getString("battel");
        }

        res = name + "," + role + "," + battel;

        // 把数据放入缓存
        cache.put(id,res);

        return res;
    }

}
