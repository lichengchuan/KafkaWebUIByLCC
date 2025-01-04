package com.lcc.kafkaUI.entity;

import lombok.Data;

import java.util.List;

@Data
public class ConsumerClient {
    //消费组内的成员编号
    private String memberId;
    //消费者客户端id
    private String clientId;
    //消费者ip
    private String host;
    //消费者订阅的TopicPartition列表
    private List<ConsumerTopicPartition> topicPartitionList;
}
