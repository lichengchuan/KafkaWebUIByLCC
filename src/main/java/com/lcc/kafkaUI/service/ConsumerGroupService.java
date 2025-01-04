package com.lcc.kafkaUI.service;

import com.lcc.kafkaUI.dto.ConsumerGroup.ConsumerGroupOffsetDto;
import com.lcc.kafkaUI.entity.ConsumerGroup;
import com.lcc.kafkaUI.entity.ConsumerGroupTopicPartition;

import java.util.List;

public interface ConsumerGroupService {
    //获取消费者组列表
    List<ConsumerGroup> getConsumerGroupList();

    //获取消费者组所有Topic消费偏移量
    List<ConsumerGroupTopicPartition> getConsumerGroupOffset(String groupId);

    //删除消费者组
    String deleteConsumerGroups(List<String> groupIds);

    //修改偏移量
    String updateCommittedOffset(ConsumerGroupOffsetDto consumerGroupOffsetDto);


}
