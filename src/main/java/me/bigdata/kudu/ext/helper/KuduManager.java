package me.bigdata.kudu.ext.helper;

import lombok.extern.slf4j.Slf4j;
import me.bigdata.kudu.ext.configuration.ConfigsHelper;
import me.bigdata.kudu.ext.configuration.DBGroup;
import me.bigdata.kudu.ext.configuration.DBGroupConfig;
import org.apache.commons.lang3.Validate;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
import org.apache.kudu.shaded.com.google.common.collect.Lists;
import org.apache.kudu.shaded.com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class KuduManager {

    private static volatile KuduManager instance;

    private static ConcurrentMap<String, KuduClient> kuduClientMap = Maps.newConcurrentMap();
    private static ConcurrentMap<String, Map<String, List<String>>> tableMap = Maps.newConcurrentMap();
    private static ConcurrentMap<String, KuduTable> kuduTableMap = Maps.newConcurrentMap();
    private static ConcurrentMap<String, List<String>> tableMatedataMap = Maps.newConcurrentMap();
    private static KuduClient kuduClient;

    private static String catalog;
    private static String schema;

    private KuduManager() {
    }

    public static KuduManager getInstance() {
        if (instance == null) {
            synchronized (KuduManager.class) {
                if (instance == null) {
                    instance = new KuduManager();
                    init();
                }
            }
        }
        return instance;
    }

    /**
     * kudu 初始化
     */
    private static void init() {
        DBGroupConfig dbGroupConfig = ConfigsHelper.getDbGroupConfig();
        List<DBGroup> dbGroupList = dbGroupConfig.getDbGroupList();
        Validate.notNull(dbGroupList, " db group don't been configed");
        for (DBGroup dbGroup : dbGroupList) {
            try {
                String servers = dbGroup.getServers();

                kuduClient = new KuduClient.KuduClientBuilder(servers)
                        .defaultOperationTimeoutMs(dbGroup.getDefaultOperationTimeoutMs())
                        .defaultSocketReadTimeoutMs(dbGroup.getDefaultSocketReadTimeoutMs())
                        .defaultAdminOperationTimeoutMs(dbGroup.getDefaultAdminOperationTimeoutMs()).build();

                //@ TODO 抓取kudu异常
                ListTabletServersResponse tServers = kuduClient.listTabletServers();
                ListTablesResponse tableList = kuduClient.getTablesList();
                kuduClientMap.putIfAbsent(dbGroup.getName(), kuduClient);

                Map<String, List<String>> tableNameMap = null;
                for (String tableNameWithDB : tableList.getTablesList()) {
                    List<String> tableNameList = Lists.newArrayList();
                    if (tableNameMap == null || !tableNameMap.containsKey(schema)) {
                        tableNameMap = Maps.newHashMap();
                    } else {
                        tableNameMap = tableMap.get(catalog);
                    }
                    String[] v1 = tableNameWithDB.split("::");
                    if (v1.length == 2) {
                        catalog = v1[0];
                        String[] v2 = v1[1].split("\\.");
                        if (v2.length == 2) {
                            schema = v2[0];
                            String tableName = v2[1];
                            if (tableNameMap.containsKey(schema)) {
                                tableNameList = tableNameMap.get(schema);
                            }
                            tableNameList.add(tableName);
                        }
                    }
                    tableNameMap.put(schema, tableNameList);
                    tableMap.put(catalog, tableNameMap);
                }
            } catch (KuduException e) {
                log.error("init kudu server error...");
            }
        }
    }

    /**
     * get client
     *
     * @return
     */
    public KuduClient getClient() {
        return kuduClient;
    }

    /**
     * table cache
     *
     * @param tableName
     * @return
     */
    public KuduTable getTable(String tableName) {
        KuduTable table = kuduTableMap.get(tableName);
        try {
            // 先判断表是否存在
            if (table == null) {
                if (kuduClient.tableExists(tableName)) {
                    table = kuduClient.openTable(tableName);
                    kuduTableMap.put(tableName, table);
                } else {
                    return null;
                }
            }
        } catch (KuduException e) {
            log.info("Kudu client open table \"" + tableName + "\" is not exists. ");
            return null;
        }
        return table;
    }

    public void removeTable(String finalTableName) {
        kuduTableMap.remove(finalTableName);
    }

    /**
     * 得到表列信息
     *
     * @param tableName
     * @return
     */
    public List<String> getTableMatedata(String tableName) {
        List<String> tableMatedata = tableMatedataMap.get(tableName);
        try {
            if (tableMatedata == null && kuduClient.tableExists(tableName)) {
                tableMatedata = Lists.newArrayList();
                KuduTable kuduTable = kuduClient.openTable(tableName);
                Schema schema = kuduTable.getSchema();
                List<ColumnSchema> columnSchemas = schema.getColumns();
                for (ColumnSchema columnSchema : columnSchemas) {
                    tableMatedata.add(columnSchema.getName());
                }
                tableMatedataMap.put(tableName, tableMatedata);
            }
        } catch (KuduException e) {
            log.info("Kudu client open table \"" + tableName + "\" is not exists. ");
        }
        return tableMatedata;
    }

    /**
     * FlushMode:AUTO_FLUSH_BACKGROUND
     *
     * @return
     */
    public KuduSession newAsyncSession() {
        KuduSession session = kuduClient.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
        session.setFlushInterval(200);
        session.setMutationBufferSpace(500);
        return session;
    }

    /**
     * 关闭单个连接
     *
     * @param connectPk
     * @return
     */
    public boolean disConnect(String connectPk) {
        KuduClient client = kuduClientMap.get(connectPk);
        try {
            client.close();
            if (kuduClientMap.containsKey(connectPk)) {
                kuduClientMap.remove(connectPk);
            }
            if (kuduClientMap.containsKey(connectPk)) {
                kuduClientMap.remove(connectPk);
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 测试连接
     *
     * @param kuduAddress
     * @return
     */
    public boolean testConnect(String kuduAddress) {
        KuduClient client = new KuduClient.KuduClientBuilder(kuduAddress).defaultSocketReadTimeoutMs(3000).build();
        try {
            ListTabletServersResponse servers = client.listTabletServers();
            if (servers != null) {
                return true;
            } else {
                return false;
            }
        } catch (KuduException e) {
            return false;
        }
    }

    /**
     * 获取表列表
     *
     * @param connectPk
     * @return
     */
    public List<String> getTableList(String connectPk) {
        if (kuduClientMap.containsKey(connectPk)) {
            ListTablesResponse tableList;
            try {
                tableList = kuduClientMap.get(connectPk).getTablesList();
                tableMap.remove(connectPk);
                return tableList.getTablesList();
            } catch (KuduException e) {
                return new ArrayList<>();
            }
        }
        return new ArrayList<>();
    }

    public static void main(String[] args) {
        KuduManager.getInstance().getTable("presto::a006.tel_test_demo");
        KuduManager.getInstance().getTableMatedata("presto::a006.tel_test_demo");
    }
}
