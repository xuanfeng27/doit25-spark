package cn.doitedu.task2worker;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class TaskWrapper implements Runnable{
    private Task task;

    public TaskWrapper(Task task) throws IOException {
        this.task = task;
        BufferedWriter bw = new BufferedWriter(new FileWriter("/tmp/shuffle/task.output63"));
        task.setWriter(bw);

    }

    @Override
    public void run() {
        try {
            task.run();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
