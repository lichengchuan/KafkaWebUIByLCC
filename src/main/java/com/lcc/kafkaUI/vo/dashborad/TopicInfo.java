package com.lcc.kafkaUI.vo.dashborad;

import lombok.Data;

@Data
public class TopicInfo {
    String topicName;
    Integer partitionNum;
    Integer messageNum;
}
