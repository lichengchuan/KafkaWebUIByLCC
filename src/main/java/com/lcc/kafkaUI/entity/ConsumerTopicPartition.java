package com.lcc.kafkaUI.entity;

import lombok.Data;

@Data
public class ConsumerTopicPartition {
    //消费者订阅的Topic列表
    private String topic;
    //消费者被分配的分区号
    private Integer partitionNum;
}
