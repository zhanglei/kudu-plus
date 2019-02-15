package me.bigdata.kudu.ext.helper;

import lombok.Data;

import java.util.List;

@Data
public class KuduMTable {

    private String catalog;
    private String schema;
    private String tableName;
    private String newTableName;
    private List<KuduColumn> rows;
    private AlterTableEnum alterTableEnum = AlterTableEnum.NONE;

    public String getCatalog() {
        return catalog;
    }

    public KuduMTable setCatalog(String catalog) {
        this.catalog = catalog;
        return this;
    }

    public String getSchema() {
        return schema;
    }

    public KuduMTable setSchema(String schema) {
        this.schema = schema;
        return this;
    }

    public String getTableName() {
        return tableName;
    }

    public KuduMTable setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public String getNewTableName() {
        return newTableName;
    }

    public KuduMTable setNewTableName(String newTableName) {
        this.newTableName = newTableName;
        return this;
    }

    public List<KuduColumn> getRows() {
        return rows;
    }

    public KuduMTable setRows(List<KuduColumn> rows) {
        this.rows = rows;
        return this;
    }

    public AlterTableEnum getAlterTableEnum() {
        return alterTableEnum;
    }

    public KuduMTable setAlterTableEnum(AlterTableEnum alterTableEnum) {
        this.alterTableEnum = alterTableEnum;
        return this;
    }

    public enum AlterTableEnum {
        DROP_TABLE("DROP_TABLE", "删除表"),
        RENAME_TABLE("RENAME_TABLE", "重命名表"),
        NONE("NONE", "不做操作");

        private String type;
        private String desc;

        AlterTableEnum(String type, String desc) {
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
}
