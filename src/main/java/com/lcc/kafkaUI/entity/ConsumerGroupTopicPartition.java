package com.lcc.kafkaUI.entity;

import lombok.Data;

@Data
public class ConsumerGroupTopicPartition {
    //所属的消费者组id
    private String groupId;
    //消费者订阅的Topic
    private String topic;
    //消费者被分配的分区号
    private Integer partitionNum;
    //当前组内对于这个topic分区的消费总量
    private Long logEndOffset;
    //当前组内对于这个topic分区的已提交偏移量
    private Long committedOffset;
}
