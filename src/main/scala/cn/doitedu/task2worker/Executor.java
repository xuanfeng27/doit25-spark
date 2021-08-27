package cn.doitedu.task2worker;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
