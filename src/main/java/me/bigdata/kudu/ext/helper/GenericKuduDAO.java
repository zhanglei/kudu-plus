package me.bigdata.kudu.ext.helper;

import lombok.extern.slf4j.Slf4j;
import me.bigdata.kudu.ext.annotation.KuduID;
import me.bigdata.kudu.ext.annotation.NonPersistent;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.apache.kudu.shaded.com.google.common.collect.Lists;
import org.apache.kudu.shaded.com.google.common.collect.Maps;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public abstract class GenericKuduDAO<T> {

    private final Class<T> clz;
    private String tableType;
    private final KuduManager kuduManager;

    public GenericKuduDAO() {
        kuduManager = KuduManager.getInstance();
        clz = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    public String getTableType() {
        return tableType;
    }

    public void setTableType(String tableType) {
        this.tableType = tableType;
    }

    public KuduTable create(KuduMTable dbObject) {
        // 设置表的schema
        List<ColumnSchema> columns = Lists.newArrayList();
        // 分区字段
        List<String> partitionColumns = new LinkedList<>();
        for (KuduColumn columnEntity : dbObject.getRows()) {
            if (!columnEntity.isNonPersistent()) {
                columns.add(KuduAgentUtils.newColumn(columnEntity.getColumnName(), columnEntity.getColumnType(), columnEntity.isPrimaryKey()));
                if (columnEntity.isPrimaryKey()) {
                    partitionColumns.add(columnEntity.getColumnName());
                }
            }
        }
        String tableName = StringUtils.isNotBlank(dbObject.getTableName()) ? dbObject.getTableName() : getTableName();
        String finalTableName = KuduAgentUtils.getFinalTableName(dbObject.getCatalog(), dbObject.getSchema(), tableName);

        Schema schema = new Schema(columns);

        CreateTableOptions options = new CreateTableOptions();
        options.addHashPartitions(partitionColumns, 3);

        try {
            kuduManager.getClient().createTable(finalTableName, schema, options);

            if (kuduManager.getClient().isCreateTableDone(finalTableName)) {
                log.info("Kudu client create \"" + finalTableName + "\" done. ");
                return kuduManager.getTable(finalTableName);
            }
        } catch (KuduException e) {
            log.error("Kudu client create table \"" + finalTableName + "\" fail. ");
            throw new CustomerException(ExceptionEnum.OPERATION_ERROR, e);
        }
        return null;
    }

    public KuduTable alter(KuduMTable dbObject) {
        String tableName = StringUtils.isNotBlank(dbObject.getTableName()) ? dbObject.getTableName() : getTableName();
        String finalTableName = KuduAgentUtils.getFinalTableName(dbObject.getCatalog(), dbObject.getSchema(), tableName);

        List<KuduColumn> kuduColumns = dbObject.getRows();
        List<String> newColumns = Lists.newArrayList();
        Map<String, Type> newColumnMap = Maps.newHashMap();
        for(KuduColumn kuduColumn : kuduColumns) {
            if (!kuduColumn.isNonPersistent()) {
                newColumns.add(kuduColumn.getColumnName());
                newColumnMap.put(kuduColumn.getColumnName(), kuduColumn.getColumnType());
            }
        }
        List<String> columns = kuduManager.getTableMatedata(finalTableName);
        // 比较找出新增的字段
        List<String> listCompares = KuduAgentUtils.listCompare(newColumns, columns);

        if (listCompares == null || listCompares.size() == 0) {
            return kuduManager.getTable(finalTableName);
        }

        AlterTableOptions alterTableOptions = new AlterTableOptions();
        for (String columnName : listCompares) {
            alterTableOptions.addColumn(KuduAgentUtils.newColumn(columnName, newColumnMap.get(columnName), false));
        }
        try {
            kuduManager.getClient().alterTable(finalTableName, alterTableOptions);
            // 修改表先移除缓存中的KuduTable
            kuduManager.removeTable(finalTableName);
            return kuduManager.getTable(finalTableName);
        } catch (KuduException e) {
            log.error("Kudu client alter table \"" + finalTableName + "\" fail. ");
            throw new CustomerException(ExceptionEnum.OPERATION_ERROR, e);
        }
    }

    public void update(T obj) {
        List objs = Lists.newArrayList();
        objs.add(obj);
        this.upsert(objs, KuduOPEnum.UPDATE);
    }

    public void update(List<T> objs) {
        this.upsert(objs, KuduOPEnum.UPDATE);
    }

    public void insert(T obj) {
        List objs = Lists.newArrayList();
        objs.add(obj);
        this.upsert(objs, KuduOPEnum.INSERT);
    }

    public void insert(List<T> objs) {
        this.upsert(objs, KuduOPEnum.INSERT);
    }

    /**
     * 插入更新
     *
     * @param objs
     */
    public void upsert(List<T> objs, KuduOPEnum kuduOPEnum) {
        List<KuduMTable> entities = Lists.newArrayList();
        for (T obj : objs) {
            KuduMTable entity = convertToObject(obj, kuduOPEnum);
            entities.add(entity);
        }
        KuduTable kuduTable = getKuduTable(entities.get(0));
        KuduSession kuduSession = kuduManager.newAsyncSession();
        // Upsert表示如果存在相同主键列就进行更新，如果不存在则进行插入，也可以直接使用Insert
        try {
            for (KuduMTable entity : entities) {
                Upsert upsert = kuduTable.newUpsert();
                KuduAgentUtils.operate(entity, upsert, kuduSession);
            }
        } catch (KuduException e) {
            log.error("kudu执行插入操作失败，失败信息:cause-->{},message-->{}", e.getCause(), e.getMessage());
            throw new CustomerException(ExceptionEnum.OPERATION_ERROR, e);
        } finally {
            KuduAgentUtils.close(kuduSession, null);
        }
    }

    public void delete(T obj) {
        List objs = Lists.newArrayList();
        objs.add(obj);
        this.delete(objs);
    }

    public void delete(List<T> objs) {
        List<KuduMTable> entities = Lists.newArrayList();
        for (T obj : objs) {
            KuduMTable entity = convertToObject(obj, KuduOPEnum.DELETE);
            entities.add(entity);
        }
        KuduMTable entity = convertToObject(objs.get(0), KuduOPEnum.DELETE);
        KuduTable kuduTable = getKuduTable(entity);
        KuduSession kuduSession = kuduManager.newAsyncSession();
        try {
            Delete delete = kuduTable.newDelete();
            KuduAgentUtils.operate(entity, delete, kuduSession);
        } catch (KuduException e) {
            log.error("kudu执行删除操作失败，失败信息:cause-->{},message-->{}", e.getCause(), e.getMessage());
            throw new CustomerException(ExceptionEnum.OPERATION_ERROR, e);
        } finally {
            KuduAgentUtils.close(kuduSession, null);
        }
    }

    public List<T> find(T obj) {
        return this.find(obj, null, null, clz);
    }

    public List<T> find() {
        return this.find(null);
    }

    public void renameTable(T obj, String newTableName) {
        List<KuduMTable> entities = Lists.newArrayList();
        KuduMTable entity = convertToObject(obj, KuduOPEnum.RENAME);
        entity.setTableName(KuduAgentUtils.getFinalTableName(entity.getCatalog(), entity.getSchema(), entity.getTableName()));
        entity.setNewTableName(KuduAgentUtils.getFinalTableName(entity.getCatalog(), entity.getSchema(), newTableName));
        entity.setAlterTableEnum(KuduMTable.AlterTableEnum.RENAME_TABLE);
        entities.add(entity);
        alter(entities);
    }

    public void renameTable(String catalog, String schema, String tableName, String newTableName) {
        List<KuduMTable> entities = Lists.newArrayList();
        KuduMTable entity = new KuduMTable();
        entity.setTableName(KuduAgentUtils.getFinalTableName(catalog, schema, tableName));
        entity.setNewTableName(KuduAgentUtils.getFinalTableName(catalog, schema, newTableName));
        entity.setAlterTableEnum(KuduMTable.AlterTableEnum.RENAME_TABLE);
        entities.add(entity);
        alter(entities);
    }

    public void renameTable(KuduMTable entity) {
        List<KuduMTable> entities = Lists.newArrayList();
        entities.add(entity);
        alter(entities);
    }

    public void dropTable(T obj) {
        List<KuduMTable> entities = Lists.newArrayList();
        KuduMTable entity = convertToObject(obj, KuduOPEnum.DROP);
        entity.setTableName(KuduAgentUtils.getFinalTableName(entity.getCatalog(), entity.getSchema(), entity.getTableName()));
        entity.setAlterTableEnum(KuduMTable.AlterTableEnum.DROP_TABLE);
        entities.add(entity);
        alter(entities);
    }

    public void dropTable(String catalog, String schema, String tableName) {
        List<KuduMTable> entities = Lists.newArrayList();
        KuduMTable entity = new KuduMTable();
        entity.setTableName(KuduAgentUtils.getFinalTableName(catalog, schema, tableName));
        entity.setAlterTableEnum(KuduMTable.AlterTableEnum.DROP_TABLE);
        entities.add(entity);
        alter(entities);
    }

    public void dropTable(KuduMTable entity) {
        List<KuduMTable> entities = Lists.newArrayList();
        entities.add(entity);
        alter(entities);
    }

    public void alterColumn(List<KuduMTable> entities) {
        alter(entities);
    }

    /**
     * 针对表的操作
     * 修改表名 删除表  增加字段 删除字段
     *
     * @param entities
     */
    public void alter(List<KuduMTable> entities) {
        KuduClient client = kuduManager.getClient();
        try {
            for (KuduMTable entity : entities) {
                // 修改表名
                if (entity.getAlterTableEnum().getType().equals(KuduRow.AlterTableEnum.RENAME_TABLE.getType())) {
                    AlterTableOptions ato = new AlterTableOptions();
                    ato.renameTable(entity.getNewTableName());
                    AlterTableResponse alterTableResponse = client.alterTable(entity.getTableName(), ato);
                    log.info("Kudu table rename success. {} -- > {}", entity.getTableName(), entity.getNewTableName());
                    continue;
                }
                // 删除表
                if (entity.getAlterTableEnum().getType().equals(KuduRow.AlterTableEnum.DROP_TABLE.getType())) {
                    DeleteTableResponse deleteTableResponse = client.deleteTable(entity.getTableName());
                    log.info("Kudu table drop success. {}", entity.getTableName());
                    continue;
                }
                // 增加字段 删除字段
                AlterTableOptions ato = new AlterTableOptions();
                for (KuduColumn column : entity.getRows()) {
                    if (column.getAlterColumnEnum().getType().equals(KuduColumn.AlterColumnEnum.ADD_COLUMN.getType()) && !column.isNullAble()) {
                        ato.addColumn(column.getColumnName(), column.getColumnType(), column.getDefaultValue());
                    } else if (column.getAlterColumnEnum().getType().equals(KuduColumn.AlterColumnEnum.ADD_COLUMN.getType()) && column.isNullAble()) {
                        ato.addNullableColumn(column.getColumnName(), column.getColumnType());
                    } else if (column.getAlterColumnEnum().getType().equals(KuduColumn.AlterColumnEnum.DROP_COLUMN.getType())) {
                        ato.dropColumn(column.getColumnName());
                    } else if (column.getAlterColumnEnum().getType().equals(KuduColumn.AlterColumnEnum.RENAME_COLUMN.getType())) {
                        ato.renameColumn(column.getColumnName(), column.getNewColumnName());
                    } else {
                        continue;
                    }
                }
                AlterTableResponse alterTableResponse = client.alterTable(entity.getTableName(), ato);
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("kudu执行表alter操作失败，失败信息:cause-->{},message-->{}", e.getCause(), e.getMessage());
            throw new CustomerException(ExceptionEnum.OPERATION_ERROR, e);
        } finally {
            KuduAgentUtils.close((KuduSession) null, null);
        }
    }

    /**
     * 组建查询类
     *
     * @param obj
     * @param start
     * @param limit
     * @param cls
     * @param <V>
     * @return
     */
    public <V> List<V> find(T obj, Integer start, Integer limit, Class<V> cls) {
        List<V> rows = Lists.newArrayList();

        KuduMTable dbObject = convertToObject(obj, KuduOPEnum.FIND);
        KuduTable kuduTable = getKuduTable(dbObject);
        Schema schema = kuduTable.getSchema();

        //查找所有的列
        List<String> columnStr = schema.getColumns().stream().map(ColumnSchema::getName).collect(Collectors.toList());

        KuduScanner.KuduScannerBuilder scannerBuilder = kuduManager.getClient().newScannerBuilder(kuduTable)
                .setProjectedColumnNames(columnStr)
                .cacheBlocks(false)
                .readMode(AsyncKuduScanner.ReadMode.READ_LATEST)
                .batchSizeBytes(1024);

        KuduAgentUtils.setKuduPredicates(kuduTable, dbObject.getRows(), scannerBuilder);

        KuduScanner scanner = null;
        try {
            scanner = scannerBuilder.build();
            while (scanner.hasMoreRows()) {
                RowResultIterator curRows = scanner.nextRows();
                while (curRows.hasNext()) {
                    RowResult rowResult = curRows.next();
                    rows.add(convertFromRowResult(schema, rowResult, cls));
                }
            }
        } catch (KuduException e) {
            log.error("kudu执查询操作失败，失败信息:cause-->{},message-->{}", e.getCause(), e.getMessage());
            throw new CustomerException(ExceptionEnum.OPERATION_ERROR, e);
        } finally {
            KuduAgentUtils.close(scanner, null);
        }
        return rows;
    }

    /**
     * 转换RowResult
     *
     * @param schema
     * @param rowResult
     * @param clazz
     * @param <V>
     * @return
     */
    private <V> V convertFromRowResult(Schema schema, RowResult rowResult, Class<V> clazz) {
        V t = null;
        try {
            t = clazz.newInstance();
        } catch (Exception e) {
            throw new CustomerException(ExceptionEnum.OPERATION_ERROR, e);
        }
        Field[] selfFields = clazz.getDeclaredFields();
        Field[] parentFields = clazz.getSuperclass().getDeclaredFields();
        Method[] selfMethods = clazz.getMethods();
        Method[] parentMethods = clazz.getSuperclass().getMethods();

        Field[] fields = (Field[]) ArrayUtils.addAll(selfFields, parentFields);
        Method[] methods = (Method[]) ArrayUtils.addAll(selfMethods, parentMethods);
        for (ColumnSchema columnSchema : schema.getColumns()) {
            for (Field field : fields) {
                if (columnSchema.getName().equalsIgnoreCase(field.getName())) {
                    Method setter = KuduAgentUtils.getSetter(methods, field);
                    Object val = null;
                    if (rowResult.isNull(columnSchema.getName())) {
                    } else {
                        switch (columnSchema.getType()) {
                            case INT8:
                                val = rowResult.getByte(columnSchema.getName());
                                break;
                            case INT16:
                                val = rowResult.getShort(columnSchema.getName());
                                break;
                            case INT32:
                                val = rowResult.getInt(columnSchema.getName());
                                break;
                            case INT64:
                            case UNIXTIME_MICROS:
                                // 由于long值返回前端进行json转换会丢失精度，所以转换为字符串返回
                                val = String.valueOf(rowResult.getLong(columnSchema.getName()));
                                break;
                            case BINARY:
                                // 二进制字段不显示
                                val = "[BINARY]";
                                break;
                            case STRING:
                                val = rowResult.getString(columnSchema.getName());
                                break;
                            case BOOL:
                                val = rowResult.getBoolean(columnSchema.getName());
                                break;
                            case FLOAT:
                                val = rowResult.getFloat(columnSchema.getName());
                                break;
                            case DOUBLE:
                                val = rowResult.getDouble(columnSchema.getName());
                                break;
                            case DECIMAL:
                                val = rowResult.getDecimal(columnSchema.getName());
                                break;
                            default:
                                val = rowResult.getString(columnSchema.getName());
                                break;
                        }
                    }
                    try {
                        Object convertedVal = null;
                        if (val instanceof Byte) {
                            convertedVal = String.valueOf(val);
                        } else if (val instanceof Integer) {
                            convertedVal = Integer.valueOf(val.toString());
                        } else {
                            convertedVal = val;
                        }
                        setter.invoke(t, convertedVal);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;
                }
            }
        }
        return t;
    }

    private List<Object> findRow(Schema schema, RowResult rowResult) {
        List<Object> row = new ArrayList<>();
        for (ColumnSchema columnSchema : schema.getColumns()) {
            Map<String, Object> map = Maps.newHashMap();
            if (rowResult.isNull(columnSchema.getName())) {
                row.add(null);
            } else {
                switch (columnSchema.getType()) {
                    case INT8:
                        map.put(columnSchema.getName(), rowResult.getByte(columnSchema.getName()));
                        break;
                    case INT16:
                        map.put(columnSchema.getName(), rowResult.getShort(columnSchema.getName()));
                        break;
                    case INT32:
                        map.put(columnSchema.getName(), rowResult.getInt(columnSchema.getName()));
                        break;
                    case INT64:
                    case UNIXTIME_MICROS:
                        // 由于long值返回前端进行json转换会丢失精度，所以转换为字符串返回
                        map.put(columnSchema.getName(), String.valueOf(rowResult.getLong(columnSchema.getName())));
                        break;
                    case BINARY:
                        // 二进制字段不显示
                        map.put(columnSchema.getName(), "[BINARY]");
                        break;
                    case STRING:
                        map.put(columnSchema.getName(), rowResult.getString(columnSchema.getName()));
                        break;
                    case BOOL:
                        map.put(columnSchema.getName(), rowResult.getBoolean(columnSchema.getName()));
                        break;
                    case FLOAT:
                        map.put(columnSchema.getName(), rowResult.getFloat(columnSchema.getName()));
                        break;
                    case DOUBLE:
                        map.put(columnSchema.getName(), rowResult.getDouble(columnSchema.getName()));
                        break;
                    case DECIMAL:
                        map.put(columnSchema.getName(), rowResult.getDecimal(columnSchema.getName()));
                        break;
                    default:
                        map.put(columnSchema.getName(), rowResult.getString(columnSchema.getName()));
                        break;
                }
            }
            row.add(map);
        }
        return row;
    }

    /**
     * 得到kudu table
     *
     * @param dbObject
     * @return
     */
    private KuduTable getKuduTable(KuduMTable dbObject) {
        // 如果没有设置table name,则使用类别为表名
        String tableName = StringUtils.isNotBlank(dbObject.getTableName()) ? dbObject.getTableName() : getTableName();
        String finalTableName = KuduAgentUtils.getFinalTableName(dbObject.getCatalog(), dbObject.getSchema(), tableName);
        KuduTable kuduTable = kuduManager.getTable(finalTableName);
        // 如果表不存在，需创建
        if (kuduTable == null) {
            kuduTable = create(dbObject);
        } else {
            kuduTable = alter(dbObject);
        }
        return kuduTable;
    }

    /**
     * 取得表名
     *
     * @return
     */
    public String getTableName() {
        String name = clz.getName();
        String[] array = name.split("\\.");
        String originalName = array[array.length - 1];
        if (StringUtils.isNotBlank(this.tableType)) {
            return originalName + this.tableType;
        } else {
            return originalName;
        }
    }

    /**
     * 转换成KuduMTable
     *
     * @param obj
     * @return
     */
    protected KuduMTable convertToObject(Object obj, KuduOPEnum kuduOPEnum) {
        KuduMTable tableEntity = new KuduMTable();
        Field[] fields = obj == null ? clz.getDeclaredFields() : obj.getClass().getDeclaredFields();
        Field[] parentFields = obj == null ? clz.getSuperclass().getDeclaredFields() : obj.getClass().getSuperclass().getDeclaredFields();
        Method[] methods = obj == null ? clz.getMethods() : obj.getClass().getMethods();
        Method[] parentMethods = obj == null ? clz.getSuperclass().getMethods() : obj.getClass().getSuperclass().getMethods();

        Field[] mergedFields = (Field[]) ArrayUtils.addAll(fields, parentFields);
        Method[] mergedMethods = (Method[]) ArrayUtils.addAll(methods, parentMethods);
        List<KuduColumn> rows = Lists.newArrayList();
        for (Field field : mergedFields) {
            KuduColumn columnEntity = new KuduColumn();
            Method getter = KuduAgentUtils.getGetter(mergedMethods, field);
            try {
                if (getter != null) {
                    // 设置 catalog/schema
                    String fieldName = field.getName();
                    // 如果obj传入空时，用clz反射出一个新对象，取出 catalog/schema
                    Object value = obj == null ? getter.invoke(clz.newInstance()) : getter.invoke(obj);
                    if (KuduConstants.CATALOG.equals(fieldName)) {
                        tableEntity.setCatalog(String.valueOf(value));
                    }
                    if (KuduConstants.SCHEMA.equals(fieldName)) {
                        tableEntity.setSchema(String.valueOf(value));
                    }
                    if (KuduConstants.TABLE_NAME.equals(fieldName)) {
                        tableEntity.setTableName(String.valueOf(value));
                    }
                    //
                    columnEntity.setColumnName(fieldName).setColumnValue(value);
                    columnEntity.setComparisonValue(value);
                    columnEntity.setColumnType(KuduAgentUtils.getColumnType(getter.getReturnType()));
                    // 如果更新/删除操作，并且value为空，将isUpdate设为false;
                    columnEntity.setUpdate((value == null && (KuduOPEnum.UPDATE.getType().equalsIgnoreCase(kuduOPEnum.getType()) || KuduOPEnum.DELETE.getType().equalsIgnoreCase(kuduOPEnum.getType()))) ? false : true);
                    if (field.isAnnotationPresent(KuduID.class)) {
                        columnEntity.setPrimaryKey(true);
                    } else if (field.isAnnotationPresent(NonPersistent.class)) {
                        columnEntity.setNonPersistent(true);
                    }
                    rows.add(columnEntity);
                }
            } catch (Exception e) {
                throw new CustomerException(ExceptionEnum.OPERATION_ERROR, "Error in invoking getter of Field [" + field.getName() + "] of class [" + clz.getName() + "]");
            }
        }
        tableEntity.setRows(rows);
        return tableEntity;
    }

    protected List<T> fillObject(List<Object> objectList) {
        return null;
    }
}
