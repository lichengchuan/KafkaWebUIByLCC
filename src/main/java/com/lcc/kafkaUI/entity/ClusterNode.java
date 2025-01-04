package com.lcc.kafkaUI.entity;

import lombok.Data;

@Data
public class ClusterNode {
    // 每台Broker的唯一标识
    private String id;
    private String host;
    private Integer port;
    // Broker 所在机架的信息，用于支持 Kafka 的机架感知特性，可以优化分区副本分布策略，以提高容错性和数据可用性。
    private String rack;
}
