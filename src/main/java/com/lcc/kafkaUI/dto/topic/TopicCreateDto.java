package com.lcc.kafkaUI.dto.topic;

import lombok.Data;

@Data
public class TopicCreateDto {
    private String topicName;
    private Integer partitions;
    private Short replication;
}
