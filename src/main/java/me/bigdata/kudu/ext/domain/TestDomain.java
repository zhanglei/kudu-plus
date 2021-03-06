package me.bigdata.kudu.ext.domain;

import me.bigdata.kudu.ext.annotation.KuduID;

public class TestDomain extends KuduBaseDomain {

    @KuduID
    private String id;
    private String userName;
    private Integer age;
    private Integer sex;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public Integer getSex() {
        return sex;
    }

    public void setSex(Integer sex) {
        this.sex = sex;
    }
}
