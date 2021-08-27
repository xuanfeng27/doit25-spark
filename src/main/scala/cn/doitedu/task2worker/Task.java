package cn.doitedu.task2worker;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Serializable;

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-08-27
 * @desc 封装任务的计算逻辑的bean
 */
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
