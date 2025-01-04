package com.lcc.kafkaUI.common.utils;

import com.lcc.kafkaUI.entity.ClusterNode;
import com.lcc.kafkaUI.entity.ConsumerClient;
import com.lcc.kafkaUI.entity.ConsumerGroup;
import com.lcc.kafkaUI.entity.ConsumerTopicPartition;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class DataTransferUtil {
    public static ClusterNode nodeToClusterNode(Node node){
        ClusterNode clusterNode = new ClusterNode();

        clusterNode.setId(node.idString());
        clusterNode.setHost(node.host());
        clusterNode.setPort(node.port());
        clusterNode.setRack(node.rack());

        return clusterNode;
    }

    public static List<ClusterNode> nodeListToClusterNode(List<Node> nodeList){
        List<ClusterNode> clusterNodeList = new ArrayList<>();
        for (Node node : nodeList) {
            clusterNodeList.add(nodeToClusterNode(node));
        }
        return clusterNodeList;
    }

    public static List<ConsumerGroup> consumerGroupDescriptionToConsumerGroup(Collection<ConsumerGroupDescription> consumerGroupDescriptionList){
        List<ConsumerGroup> consumerGroupList = new ArrayList<>();
        for (ConsumerGroupDescription consumerGroupDescription : consumerGroupDescriptionList) {
            ConsumerGroup consumerGroup = new ConsumerGroup();
            consumerGroup.setGroupId(consumerGroupDescription.groupId());
            consumerGroup.setSimpleConsumerGroup(consumerGroupDescription.isSimpleConsumerGroup());
            consumerGroup.setState(consumerGroupDescription.state().toString());
            consumerGroup.setPartitionAssignor(consumerGroupDescription.partitionAssignor());
            ClusterNode clusterNode = nodeToClusterNode(consumerGroupDescription.coordinator());
            consumerGroup.setCoordinator(clusterNode);
            Collection<MemberDescription> members = consumerGroupDescription.members();
            List<ConsumerClient> consumerClientList = memberDescriptionToConsumerClient(members);
            consumerGroup.setMembers(consumerClientList);
            consumerGroupList.add(consumerGroup);
        }
        return consumerGroupList;
    }

    public static List<ConsumerClient> memberDescriptionToConsumerClient(Collection<MemberDescription> memberDescriptionList){
        List<ConsumerClient> consumerClientList = new ArrayList<>();
        for (MemberDescription memberDescription : memberDescriptionList) {
            ConsumerClient consumerClient = new ConsumerClient();
            consumerClient.setClientId(memberDescription.clientId());
            consumerClient.setHost(memberDescription.host());
            consumerClient.setMemberId(memberDescription.consumerId());
            Set<TopicPartition> topicPartitions = memberDescription.assignment().topicPartitions();
            List<ConsumerTopicPartition> consumerTopicPartitionList = topicPartitionToConsumerTopicPartition(topicPartitions);
            consumerClient.setTopicPartitionList(consumerTopicPartitionList);
            consumerClientList.add(consumerClient);
        }
        return consumerClientList;
    }

    public static List<ConsumerTopicPartition> topicPartitionToConsumerTopicPartition(Set<TopicPartition> topicPartitionList){
        List<ConsumerTopicPartition> consumerTopicPartitionList = new ArrayList<>();
        for (TopicPartition topicPartition : topicPartitionList) {
            ConsumerTopicPartition consumerTopicPartition = new ConsumerTopicPartition();
            consumerTopicPartition.setTopic(topicPartition.topic());
            consumerTopicPartition.setPartitionNum(topicPartition.partition());
            consumerTopicPartitionList.add(consumerTopicPartition);
        }
        return consumerTopicPartitionList;
    }
}
