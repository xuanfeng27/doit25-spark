package cn.doitedu.task2worker;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Serializable;

public class Task implements Serializable {

    private int num;
    transient private BufferedWriter writer;

    public Task(int num) {
        this.num = num;
    }

    public void run() throws IOException {
        for(int i = 0; i < num ; i++ ){
            writer.write(i+" - ");
        }
        writer.close();
    }



    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public BufferedWriter getWriter() {
        return writer;
    }

    public void setWriter(BufferedWriter writer) {
        this.writer = writer;
    }


}
