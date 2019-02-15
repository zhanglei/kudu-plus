package me.bigdata.kudu.ext.configuration;

import lombok.extern.slf4j.Slf4j;

import java.util.Properties;

@Slf4j
public class ConfigsHelper {

    private static DBGroupConfig dbGroupConfig = new DBGroupConfig();

    static {
        try {
            Properties properties = new Properties();
            properties.load(ConfigsHelper.class.getResourceAsStream("/kudu.properties"));
            String kuduAddress = properties.getProperty("kudu-address");
            String defaultOperationTimeoutMs = properties.getProperty("default-operation-timeout-ms");
            String defaultSocketReadTimeoutMs = properties.getProperty("default-socket-read-timeout-ms");
            String defaultAdminOperationTimeoutMs = properties.getProperty("default-admin-operation-timeout-ms");
            String flushInterval = properties.getProperty("flush-interval");
            String mutationBufferSpace = properties.getProperty("mutation-buffer-space");

            DBGroup dbGroup = new DBGroup();
            dbGroup.setServers(kuduAddress);
            dbGroup.setDefaultOperationTimeoutMs(Integer.valueOf(defaultOperationTimeoutMs));
            dbGroup.setDefaultSocketReadTimeoutMs(Integer.valueOf(defaultSocketReadTimeoutMs));
            dbGroup.setDefaultAdminOperationTimeoutMs(Integer.valueOf(defaultAdminOperationTimeoutMs));
            dbGroup.setFlushInterval(Integer.valueOf(flushInterval));
            dbGroup.setMutationBufferSpace(Integer.valueOf(mutationBufferSpace));

            dbGroupConfig.addDbGroup(dbGroup);
        } catch (Exception e) {
            log.error("kudu config error...");
        }
    }

    public static DBGroupConfig getDbGroupConfig() {
        return dbGroupConfig;
    }
}
