package com.yang.apitest.pojo;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author zhangyang03
 * @Description Topic001 的java 类
 * @create 2022-04-27 10:42
 */
public class Topic001 {
    Integer id;
    Date time;
    Integer num;

    public Topic001() {
    }

    public Topic001(Integer id, Date time, Integer num) {
        this.id = id;
        this.time = time;
        this.num = num;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
    }

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }

    @Override
    public String toString() {
        return "Topic001{" +
                "id=" + id +
                ", time=" + new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSSS").format(time) +
                ", num=" + num +
                '}';
    }
}
