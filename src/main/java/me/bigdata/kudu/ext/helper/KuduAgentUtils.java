package me.bigdata.kudu.ext.helper;

import lombok.extern.slf4j.Slf4j;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static me.bigdata.kudu.ext.helper.KuduConstants.*;

@Slf4j
public class KuduAgentUtils {

    /**
     * 通用方法
     *
     * @param entity
     * @param operate
     * @param session
     * @return
     */
    public static OperationResponse operate(KuduMTable entity, Operation operate, KuduSession session) throws KuduException {
        for (KuduColumn column : entity.getRows()) {
            KuduAgentUtils.WrapperKuduOperation(column, operate);
        }
        OperationResponse apply = session.apply(operate);
        return apply;
    }

    public static Operation WrapperKuduOperation(KuduColumn entity, Operation operate) {
        Type rowType = entity.getColumnType();
        String columnName = entity.getColumnName();
        Object columnValue = entity.getColumnValue();
        // 如果是 NonPersistent isUpdate为false的字段 直接返回
        if (entity.isNonPersistent() || !entity.isUpdate()) {
            return operate;
        }
        log.info("kudu操作对象包装，列名:{},列值:{}", columnName, columnValue);
        if (rowType.equals(Type.BINARY)) {
        }
        if (rowType.equals(Type.STRING)) {
            if (isSetLogic(entity, operate)) {
                operate.getRow().addString(columnName, String.valueOf(columnValue));
            }
        }
        if (rowType.equals(Type.BOOL)) {
        }
        if (rowType.equals(Type.DOUBLE)) {
        }
        if (rowType.equals(Type.FLOAT)) {
        }
        if (rowType.equals(Type.INT8)) {
        }
        if (rowType.equals(Type.INT16)) {
        }
        if (rowType.equals(Type.INT32)) {
            if (isSetLogic(entity, operate)) {
                operate.getRow().addInt(columnName, (Integer) columnValue);
            }
        }
        if (rowType.equals(Type.INT64)) {
            if (isSetLogic(entity, operate)) {
                operate.getRow().addLong(columnName, (Integer) columnValue);
            }
        }
        if (rowType.equals(Type.UNIXTIME_MICROS)) {
        }
        return operate;
    }

    /**
     * 如果是update事件并且是更新字段就设置，如果非update事件都设置
     * 如果是delete事件是主键就设置，不是主键就不设置
     *
     * @param entity
     * @param operate
     * @return
     */
    public static boolean isSetLogic(KuduColumn entity, Operation operate) {
        return ((operate instanceof Update && entity.isUpdate()) || (operate instanceof Update && entity.isPrimaryKey())) || (operate instanceof Delete && entity.isPrimaryKey()) || (!(operate instanceof Update) && !(operate instanceof Delete));
    }

    /**
     * 得到最终名称
     *
     * @param catalog
     * @param schema
     * @param tableName
     * @return
     */
    public static String getFinalTableName(String catalog, String schema, String tableName) {
        return catalog + TABLE_PREFIX + schema + DOT + tableName;
    }

    /**
     * 得到列类型
     *
     * @param returnType
     * @return
     */
    public static Type getColumnType(Class<?> returnType) {
        Type realType = null;
        switch (returnType.getName()) {
            case "java.lang.String":
                realType = Type.STRING;
                break;
            case "java.lang.Integer":
                realType = Type.INT32;
                break;
            case "java.lang.Double":
                realType = Type.DOUBLE;
                break;
            case "java.lang.Long":
                realType = Type.UNIXTIME_MICROS;
                break;
        }
        return realType;
    }

    /**
     * get 方法
     *
     * @param methods
     * @param field
     * @return
     */
    public static Method getGetter(Method[] methods, Field field) {
        for (Method m : methods) {
            String mName = m.getName().toLowerCase();
            String fName = field.getName().toLowerCase();
            if (mName.equals("get" + fName) || mName.equals("is" + fName)) {
                return m;
            }
        }
        return null;
    }

    /**
     * set 方法
     *
     * @param methods
     * @param field
     * @return
     */
    public static Method getSetter(Method[] methods, Field field) {
        for (Method m : methods) {
            String mName = m.getName().toLowerCase();
            String fName = field.getName().toLowerCase();
            if (mName.equals("set" + fName)) {
                return m;
            }
        }
        return null;
    }

    /**
     * 新建Column
     *
     * @param name
     * @param type
     * @param iskey
     * @return
     */
    public static ColumnSchema newColumn(String name, Type type, boolean iskey) {
        ColumnSchema.ColumnSchemaBuilder column = new ColumnSchema.ColumnSchemaBuilder(name, type);
        column.key(iskey);
        if (!iskey) {
            column.nullable(true);
        }
        return column.build();
    }

    /**
     * 设置条件
     *
     * @param kuduTable
     * @param entitys
     * @param kuduScannerBuilder
     */
    public static void setKuduPredicates(KuduTable kuduTable, List<KuduColumn> entitys, KuduScanner.KuduScannerBuilder kuduScannerBuilder) {
        for (KuduColumn entity : entitys) {
            if (entity.isNonPersistent()) {
                return;
            }
            if (entity.getComparisonOp() != null && entity.getComparisonValue() != null) {
                KuduPredicate kuduPredicate = null;
                switch (entity.getColumnType()) {
                    case BOOL:
                        kuduPredicate = KuduPredicate.newComparisonPredicate(kuduTable.getSchema().getColumn(entity.getColumnName()), entity.getComparisonOp(), (Boolean) entity.getComparisonValue());
                        break;
                    case FLOAT:
                        kuduPredicate = KuduPredicate.newComparisonPredicate(kuduTable.getSchema().getColumn(entity.getColumnName()), entity.getComparisonOp(), (Float) entity.getComparisonValue());
                        break;
                    case DOUBLE:
                        kuduPredicate = KuduPredicate.newComparisonPredicate(kuduTable.getSchema().getColumn(entity.getColumnName()), entity.getComparisonOp(), (Double) entity.getComparisonValue());
                        break;
                    case BINARY:
                        kuduPredicate = KuduPredicate.newComparisonPredicate(kuduTable.getSchema().getColumn(entity.getColumnName()), entity.getComparisonOp(), (byte[]) entity.getComparisonValue());
                        break;
                    case STRING:
                        kuduPredicate = KuduPredicate.newComparisonPredicate(kuduTable.getSchema().getColumn(entity.getColumnName()), entity.getComparisonOp(), (String) entity.getComparisonValue());
                        break;
                    case UNIXTIME_MICROS:
                        kuduPredicate = KuduPredicate.newComparisonPredicate(kuduTable.getSchema().getColumn(entity.getColumnName()), entity.getComparisonOp(), (Long) entity.getComparisonValue());
                        break;
                    default:
                        kuduPredicate = KuduPredicate.newComparisonPredicate(kuduTable.getSchema().getColumn(entity.getColumnName()), entity.getComparisonOp(), (Double) entity.getComparisonValue());
                        break;
                }
                kuduScannerBuilder.addPredicate(kuduPredicate);
            }
        }
    }

    /**
     * 返回查询的一行 map
     *
     * @param row
     * @param entitys
     * @return
     */
    public static Map<String, Object> convertFromRowResult(RowResult row, List<KuduColumn> entitys) {
        Map<String, Object> result = new HashMap<>();
        for (KuduColumn entity : entitys) {
            if (entity.getColumnType() != null) {
                switch (entity.getColumnType()) {
                    case BOOL:
                        result.put(entity.getColumnName(), row.getBoolean(entity.getColumnName()));
                        break;
                    case BINARY:
                        result.put(entity.getColumnName(), row.getBinary(entity.getColumnName()));
                        break;
                    case STRING:
                        result.put(entity.getColumnName(), row.getString(entity.getColumnName()));
                        break;
                    case INT8:
                        result.put(entity.getColumnName(), row.getByte(entity.getColumnName()));
                        break;
                    case INT16:
                        result.put(entity.getColumnName(), row.getShort(entity.getColumnName()));
                        break;
                    case INT32:
                        result.put(entity.getColumnName(), row.getInt(entity.getColumnName()));
                        break;
                    case INT64:
                        result.put(entity.getColumnName(), row.getLong(entity.getColumnName()));
                        break;
                    case DOUBLE:
                        result.put(entity.getColumnName(), row.getDouble(entity.getColumnName()));
                        break;
                    case FLOAT:
                        result.put(entity.getColumnName(), row.getFloat(entity.getColumnName()));
                        break;
                    case UNIXTIME_MICROS:
                        result.put(entity.getColumnName(), row.getLong(entity.getColumnName()));
                        break;
                }
            }
        }
        return result;
    }

    /**
     * session close
     *
     * @param session
     * @param client
     * @return
     */
    public static List<OperationResponse> close(KuduSession session, KuduClient client) {
        if (null != session) {
            try {
                session.flush();
                RowErrorsAndOverflowStatus error = session.getPendingErrors();
                if (error.isOverflowed() || error.getRowErrors().length > 0) {
                    if (error.isOverflowed()) {
                        throw new CustomerException(ExceptionEnum.OPERATION_ERROR, "Kudu overflow exception occurred.");
                    }
                    StringBuilder errorMessage = new StringBuilder();
                    if (error.getRowErrors().length > 0) {
                        for (RowError errorObj : error.getRowErrors()) {
                            errorMessage.append(errorObj.toString());
                            errorMessage.append(";");
                        }
                    }
                    throw new CustomerException(ExceptionEnum.OPERATION_ERROR, "[" + errorMessage.toString() + "]");
                }
                log.info("Kudu operation success.");
            } catch (KuduException e) {
                log.error("Kudu exception while operation data.", e);
                throw new CustomerException(ExceptionEnum.OPERATION_ERROR, e);
            }
        }
        List<OperationResponse> responses = null;
        if (null != session) {
            try {
                if (!session.isClosed()) {
                    session.close();
                }
            } catch (KuduException e) {
                log.error("Kudu session close.", e);
                throw new CustomerException(ExceptionEnum.OPERATION_ERROR, e);
            }
        }
        if (null != client) {
            try {
                client.close();
            } catch (KuduException e) {
                log.error("Kudu client close.", e);
                throw new CustomerException(ExceptionEnum.OPERATION_ERROR, e);
            }
        }
        return responses;
    }

    /**
     * KuduScanner close
     *
     * @param build
     * @param client
     */
    public static void close(KuduScanner build, KuduClient client) {
        if (null != build) {
            try {
                build.close();
            } catch (KuduException e) {
                log.error("Kudu build close.", e);
                throw new CustomerException(ExceptionEnum.OPERATION_ERROR, e);
            }
        }
        if (null != client) {
            try {
                client.close();
            } catch (KuduException e) {
                log.error("Kudu client close.", e);
                throw new CustomerException(ExceptionEnum.OPERATION_ERROR, e);
            }
        }
    }
}
