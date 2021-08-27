package cn.doitedu.task2worker;

import java.io.Serializable;

public class Response implements Serializable {
    private String ifSuccess;

    public String getIfSuccess() {
        return ifSuccess;
    }

    public void setIfSuccess(String ifSuccess) {
        this.ifSuccess = ifSuccess;
    }

    public Response(String ifSuccess) {
        this.ifSuccess = ifSuccess;
    }
}
