package me.bigdata.kudu.ext.configuration;

import lombok.Data;

@Data
public class DBGroup {

    //名称
    private String name = "kudu";
    //类型，默认为mongodb
    private String type = "kudu";
    //服务器列表
    private String servers;
    private Integer defaultOperationTimeoutMs;
    private Integer defaultSocketReadTimeoutMs;
    private Integer defaultAdminOperationTimeoutMs;
    private Integer flushInterval;
    private Integer mutationBufferSpace;
}
