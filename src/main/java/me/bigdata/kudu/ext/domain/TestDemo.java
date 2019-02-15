package me.bigdata.kudu.ext.domain;

import me.bigdata.kudu.ext.annotation.KuduID;
import me.bigdata.kudu.ext.annotation.NonPersistent;

public class TestDemo extends KuduBaseDomain {

    @NonPersistent
    private String tableName = "tel_test_demo";
    @KuduID
    private String id;
    private String userName;
    private Integer age;
    private Integer sex;
    private String email;
    private String qq;

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

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

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getQq() {
        return qq;
    }

    public void setQq(String qq) {
        this.qq = qq;
    }
}
