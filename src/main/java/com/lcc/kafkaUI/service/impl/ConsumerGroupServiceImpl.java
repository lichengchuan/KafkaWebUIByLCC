package com.lcc.kafkaUI.service.impl;

import com.lcc.kafkaUI.common.utils.DataTransferUtil;
import com.lcc.kafkaUI.config.AdminClientConfig;
import com.lcc.kafkaUI.dto.ConsumerGroup.ConsumerGroupOffsetDto;
import com.lcc.kafkaUI.entity.ConsumerGroup;
import com.lcc.kafkaUI.entity.ConsumerGroupTopicPartition;
import com.lcc.kafkaUI.service.ConsumerGroupService;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;

@Service
public class ConsumerGroupServiceImpl implements ConsumerGroupService {

    @Autowired
    private AdminClient adminClient;
    @Autowired
    private AdminClientConfig adminClientConfig;

    @Override
    public List<ConsumerGroup> getConsumerGroupList() {

        ListConsumerGroupsResult listConsumerGroupsResult = adminClient.listConsumerGroups();
        try {
            Collection<ConsumerGroupListing> consumerGroupListings = listConsumerGroupsResult.all().get();
            List<String> allGroupIds = new ArrayList<>();
            consumerGroupListings.forEach(consumerGroupListing -> {
                String groupId = consumerGroupListing.groupId();
                allGroupIds.add(groupId);
            });
            Map<String, ConsumerGroupDescription> map = adminClient.describeConsumerGroups(allGroupIds).all().get();
            Collection<ConsumerGroupDescription> consumerGroupDescriptions = map.values();
            List<ConsumerGroup> consumerGroupList = DataTransferUtil.consumerGroupDescriptionToConsumerGroup(consumerGroupDescriptions);
            return consumerGroupList;

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

    @Override
    public List<ConsumerGroupTopicPartition> getConsumerGroupOffset(String groupId) {

        List<ConsumerGroupTopicPartition> consumerGroupTopicPartitionList = new ArrayList<>();

        try {

            // 获取消费者组的偏移量信息
            ListConsumerGroupOffsetsResult offsetsResult = adminClient.listConsumerGroupOffsets(groupId);
            Map<TopicPartition, OffsetAndMetadata> groupOffsets = offsetsResult.partitionsToOffsetAndMetadata().get();

            // 为了获取每个分区的最新偏移量（Log End Offset），需要一个 KafkaConsumer 实例
            Properties consumerProps = new Properties();
            consumerProps.put("bootstrap.servers",adminClientConfig.getBootstrapServers());
            consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProps.put("group.id", groupId);

            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {

                // 获取所有分区的最新偏移量
                for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : groupOffsets.entrySet()) {
                    TopicPartition topicPartition = entry.getKey();
                    long committedOffset = entry.getValue().offset();

                    // 获取分区的最新偏移量（Log End Offset）
                    consumer.assign(Collections.singletonList(topicPartition));
                    consumer.seekToEnd(Collections.singletonList(topicPartition));
                    long logEndOffset = consumer.position(topicPartition);

                    long lag = logEndOffset - committedOffset;

                    System.out.println("Topic: " + topicPartition.topic() +
                            ", Partition: " + topicPartition.partition() +
                            ", Committed Offset: " + committedOffset +
                            ", Log End Offset: " + logEndOffset +
                            ", Lag: " + lag);
                    ConsumerGroupTopicPartition consumerGroupTopicPartition = new ConsumerGroupTopicPartition();
                    consumerGroupTopicPartition.setGroupId(groupId);
                    consumerGroupTopicPartition.setTopic(topicPartition.topic());
                    consumerGroupTopicPartition.setPartitionNum(topicPartition.partition());
                    consumerGroupTopicPartition.setCommittedOffset(committedOffset);
                    consumerGroupTopicPartition.setLogEndOffset(logEndOffset);
                    consumerGroupTopicPartitionList.add(consumerGroupTopicPartition);
                }
                consumer.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            return consumerGroupTopicPartitionList;
        }

    }

    @Override
    public String deleteConsumerGroups(List<String> groupIds) {
        if (groupIds.size()<=0){
            return "操作失败";
        }
        try{
            adminClient.deleteConsumerGroups(groupIds).all().get();
        }catch (Exception e){
            e.printStackTrace();
            if (e.getCause() instanceof GroupNotEmptyException){
                System.out.println("删除失败，消费者组不为空");
                return "删除失败，消费者组不为空";
            }
            System.out.println("操作失败");
            return "操作失败";
        }
        return "操作成功";
    }

    @Override
    public String updateCommittedOffset(ConsumerGroupOffsetDto consumerGroupOffsetDto) {

        // 定义新的偏移量
        TopicPartition topicPartition = new TopicPartition(consumerGroupOffsetDto.getTopic(), consumerGroupOffsetDto.getPartitionNum());
        OffsetAndMetadata newOffset = new OffsetAndMetadata(consumerGroupOffsetDto.getOffset(), "重置偏移量");

        // 构建修改偏移量的请求
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(topicPartition, newOffset);

        // 调用 API 修改偏移量
        try {
            adminClient.alterConsumerGroupOffsets(consumerGroupOffsetDto.getConsumerGroupId(), offsets).all().get();
        } catch (Exception e) {
            e.printStackTrace();
            return "操作失败";
        }


        return "操作成功";
    }

}
