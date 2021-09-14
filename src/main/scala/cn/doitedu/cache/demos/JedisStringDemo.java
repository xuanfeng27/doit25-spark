package cn.doitedu.cache.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.io.*;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
class Person implements Serializable{
    String name;
    int age;
    String gender;
}



public class JedisStringDemo {

    public static void main(String[] args) throws Exception {
        Jedis jedis = new Jedis("doit01", 6379);
        // setStringData(jedis,"name","zhangsan");
        // getStringData(jedis,"name");

         Person person = new Person("zhangsan", 18, "male");
         setObject2StringStructure(jedis,"zhangsan",person);
         Person p = getObjectFromStringStructure(jedis, "zhangsan");
         System.out.println(p);

    }


    public static void setStringData(Jedis jedis,String key,String value){
        jedis.set(key,value);

        // 如果key不存在才set，存在则不做
        jedis.setnx("name","lisi");

        // 插入一条string数据并设置存活时长（秒）
        jedis.setex(key,10L,value);

        // 插入一条string数据并设置存活时长（毫秒）
        jedis.psetex(key,10000L,value);

        jedis.set(key.getBytes(),value.getBytes());

    }


    public static void getStringData(Jedis jedis,String key){
        String value = jedis.get(key);
        System.out.println(value);


        byte[] bytes = jedis.get(key.getBytes());
        String res = new String(bytes);

    }



    public static void setObject2StringStructure(Jedis jedis,String key,Person person) throws IOException {
        // 把一个person对象，存到redis中(用string数据结构）
        ByteArrayOutputStream baout = new ByteArrayOutputStream();
        ObjectOutputStream objOut = new ObjectOutputStream(baout);
        objOut.writeObject(person);


        jedis.set(key.getBytes(),baout.toByteArray());

    }

    public static Person getObjectFromStringStructure(Jedis jedis,String key) throws IOException, ClassNotFoundException {

        byte[] bytes = jedis.get(key.getBytes());
        // 反序列化
        ByteArrayInputStream bain = new ByteArrayInputStream(bytes);
        ObjectInputStream objIn = new ObjectInputStream(bain);
        Person person = (Person) objIn.readObject();

        return person;
    }






}
