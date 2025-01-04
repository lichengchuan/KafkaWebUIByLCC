package com.lcc.kafkaUI.dto.ConsumerGroup;

import lombok.Data;

@Data
public class ConsumerGroupOffsetDto {
    private String consumerGroupId;
    private String topic;
    private Integer partitionNum;
    private Long offset;
}
