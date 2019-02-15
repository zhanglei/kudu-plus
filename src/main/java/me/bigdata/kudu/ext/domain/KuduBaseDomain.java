package me.bigdata.kudu.ext.domain;

import me.bigdata.kudu.ext.annotation.NonPersistent;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.io.Serializable;

public abstract class KuduBaseDomain implements Serializable {

    private static final long serialVersionUID = 7790145014352426472L;

    // default presto
    @NonPersistent
    public String catalog = "presto";
    // default schema
    @NonPersistent
    public String schema = "a006";
    @NonPersistent
    public String tableName = "tel_default";

    public String getCatalog() {
        return catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
