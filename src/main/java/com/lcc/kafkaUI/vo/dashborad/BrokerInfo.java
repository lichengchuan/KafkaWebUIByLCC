package com.lcc.kafkaUI.vo.dashborad;

import lombok.Data;

/**
 * 大屏看板：Broker概况
 */
@Data
public class BrokerInfo {
    //每台Broker的唯一标识
    private String id;
    //主机名
    private String host;
    //端口号
    private Integer port;
    //jmx端口号
    private Integer jmxPort;
    //当前broker上的分区数
    private Integer partitionNum;
    //JVM 堆内存使用率 0.xx
    private Double jvmUsedMemory;
    //JVM 线程数
    private Integer jvmThreads;
    //系统CPU使用率 0.xx
    private Double systemCpuLoad;
    //进程CPU使用率 0.xx
    private Double processCpuLoad;
    //总物理内存 byte
    private Long totalPhysicalMemorySize;
    //空闲物理内存 byte
    private Long freePhysicalMemorySize;
    //已用物理内存 byte
    private Long usedPhysicalMemorySize;

    // 每秒进入的字节数速率 byte/s
    private Double bytesInRate;
    // 最近1分钟进入的字节数速率 byte/s
    private Double bytesInOneMinRate;
    // 最近5分钟进入的字节数速率 byte/s
    private Double bytesIn5MinRate;
    // 最近15分钟进入的字节数速率 byte/s
    private Double bytesIn15MinRate;

    // 每秒发送的字节数速率 byte/s
    private Double bytesOutRate;
    // 最近1分钟发送的字节数速率 byte/s
    private Double bytesOutOneMinRate;
    // 最近5分钟发送的字节数速率 byte/s
    private Double bytesOut5MinRate;
    // 最近15分钟发送的字节数速率 byte/s
    private Double bytesOut15MinRate;

    // 累计消息总字节数 byte
    private Long messagesInBytes;
}
