# kudu-plus

#### 介绍
kudu-plus 主要是工作中使用kudu，对kudu client熟悉来实现的一些操作。

#### 功能实现
- 查看kudu集群所有表
- 创建kudu表
- 删除kudu表
- 重命名kudu表
- 数据插入（批量）
- 数据更新（批量）
- 数据删除（批量）
- 数据修改（批量）

public interface IGenericDAO<T extends KuduBaseDomain> {

    /**
     * 创建表
     *
     * @param obj
     */
    public KuduTable create(KuduMTable obj);

    /**
     * 更新数据
     *
     * @param obj
     */
    public void update(T obj);

    /**
     * 批量更新
     *
     * @param objs
     */
    public void update(List<T> objs);

    /**
     * 插入数据
     *
     * @param obj
     */
    public void insert(T obj);

    /**
     * 批量插入
     *
     * @param objs
     */
    public void insert(List<T> objs);

    /**
     * 删除数据(删除只能是主键)
     *
     * @param obj
     */
    public void delete(T obj);

    /**
     * 批量删除
     *
     * @param objs
     */
    public void delete(List<T> objs);

    /**
     * 查询数据
     *
     * @return
     */
    public List<T> find(T obj);

    /**
     * 查询所有数据
     *
     * @return
     */
    public List<T> find();

    /**
     * 修改表名
     *
     * @param obj
     * @param newTableName
     */
    public void renameTable(T obj, String newTableName);

    /**
     * 修改表名
     *
     * @param catalog
     * @param schema
     * @param tableName
     * @param newTableName
     */
    public void renameTable(String catalog, String schema, String tableName, String newTableName);

    /**
     * 修改表名
     *
     * @param entity
     */
    public void renameTable(KuduMTable entity);

    /**
     * 删除表
     *
     * @param obj
     */
    public void dropTable(T obj);

    /**
     * 删除表
     *
     * @param catalog
     * @param schema
     * @param tableName
     */
    public void dropTable(String catalog, String schema, String tableName);

    /**
     * 删除表
     *
     * @param entity
     */
    public void dropTable(KuduMTable entity);

    /**
     * 增加字段 删除字段
     *
     * @param entities
     */
    public void alterColumn(List<KuduMTable> entities);
}