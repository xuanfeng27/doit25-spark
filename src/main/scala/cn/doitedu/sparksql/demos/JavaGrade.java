package cn.doitedu.sparksql.demos;

import java.io.Serializable;

public class JavaGrade implements Serializable {
    private int level;
    private int stuCount;
    private String teacher;

    public JavaGrade() {
    }

    public JavaGrade(int level, int stuCount, String teacher) {
        this.level = level;
        this.stuCount = stuCount;
        this.teacher = teacher;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public int getStuCount() {
        return stuCount;
    }

    public void setStuCount(int stuCount) {
        this.stuCount = stuCount;
    }

    public String getTeacher() {
        return teacher;
    }

    public void setTeacher(String teacher) {
        this.teacher = teacher;
    }
}
