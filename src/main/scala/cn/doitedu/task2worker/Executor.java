package cn.doitedu.task2worker;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-08-27
 * @desc 负责监听端口，接收task对象，包装成一个Runnable（用了一个TaskWrapper类），并丢入一个线程池去执行
 */
public class Executor {

    public static void main(String[] args) throws IOException, ClassNotFoundException {

        ServerSocket ss = new ServerSocket(8080);
        System.out.println("executor 服务已成功启动，监听端口: 8080");
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        while (true) {
            // 等待请求
            Socket sc = ss.accept();

            InputStream in = sc.getInputStream();
            OutputStream out = sc.getOutputStream();

            ObjectInputStream objIn = new ObjectInputStream(in);
            ObjectOutputStream objOut = new ObjectOutputStream(out);

            Task task = (Task) objIn.readObject();
            TaskWrapper taskWrapper = new TaskWrapper(task);

            Response res = new Response("success");
            try {
                // 把 task 提交到线程池
                executorService.submit(taskWrapper);
            } catch (Exception e) {
                res.setIfSuccess("failed");
                e.printStackTrace();
            }

            objOut.writeObject(res);

        }
    }
}
