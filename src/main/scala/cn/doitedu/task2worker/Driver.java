package cn.doitedu.task2worker;

import java.io.*;
import java.net.Socket;

public class Driver {

    public static void main(String[] args) throws Exception {
        // hdp02:8080,hdp03:8080
        String executorServers = args[0];
        String[] servers = executorServers.split(",");



        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    String[] hostAndPort = servers[0].split(":");

                    // 生产task对象
                    Task task1 = new Task(10);
                    Socket sc = new Socket(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
                    OutputStream out = sc.getOutputStream();
                    ObjectOutput objOut = new ObjectOutputStream(out);

                    InputStream in = sc.getInputStream();
                    ObjectInputStream objIn = new ObjectInputStream(in);

                    // 发送task到executor上去
                    objOut.writeObject(task1);

                    Response response = (Response) objIn.readObject();
                    System.out.println(response.getIfSuccess());


                    out.close();
                    in.close();
                    objOut.close();
                    objIn.close();
                    sc.close();
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
        }).start();



        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    String[] hostAndPort = servers[1].split(":");

                    // 生产task对象
                    Task task1 = new Task(10);
                    Socket sc = new Socket(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
                    OutputStream out = sc.getOutputStream();
                    ObjectOutput objOut = new ObjectOutputStream(out);

                    InputStream in = sc.getInputStream();
                    ObjectInputStream objIn = new ObjectInputStream(in);

                    // 发送task到executor上去
                    objOut.writeObject(task1);

                    Response response = (Response) objIn.readObject();
                    System.out.println(response.getIfSuccess());


                    out.close();
                    in.close();
                    objOut.close();
                    objIn.close();
                    sc.close();
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
        }).start();

    }

}
