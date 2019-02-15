package me.bigdata.kudu.ext.helper;

public enum KuduOPEnum {

    INSERT("INSERT", "插入数据"),
    UPDATE("UPDATE", "更新数据"),
    DELETE("DELETE", "删除数据"),
    FIND("FIND", "查询数据"),
    RENAME("RENAME", "修改表名"),
    DROP("DROP", "删除表"),
    ALTER("ALTER", "修改表"),
    ;

    private String type;
    private String desc;

    KuduOPEnum(String type, String desc) {
        this.type = type;
        this.desc = desc;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }
}