package com.lcc.kafkaUI.test;

import com.lcc.kafkaUI.common.utils.DateUtils;
import com.lcc.kafkaUI.vo.dashborad.TopicInfo;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class TestBaseAPI {

    public static void main(String[] args) throws Exception{
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.149.128:9092");
        AdminClient adminClient = AdminClient.create(properties);


//        describeCluster(adminClient);
//        listAllTopics(adminClient);

//        createTopics(adminClient,"lcc-test");
//        listAllTopics(adminClient);

//        deleteTopics(adminClient,"lcc-test");
//        listAllTopics(adminClient);
//
//        getMessageListByTopicName("test");

//        describeTopics(adminClient);

//        listConsumerGroups(adminClient);
//
//        describeConsumerGroups(adminClient);

        testTopicInfo(adminClient);

        //关闭资源
        adminClient.close();
    }

    /**
     * 获取Kafka集群信息
     * @param adminClient
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void describeCluster(AdminClient adminClient) throws ExecutionException, InterruptedException {
        DescribeClusterResult clusterInfo = adminClient.describeCluster();

        //获取集群ID
        String clusterID = clusterInfo.clusterId().get();
        System.out.println("集群id："+clusterID);
        //获取集群控制器（controller是一个角色，任何一台broker都能充当controller）
        Node controller = clusterInfo.controller().get();
        System.out.println("控制器："+controller);

        //获取集群中的节点
        Collection<Node> nodes = clusterInfo.nodes().get();
        for (Node node : nodes) {
            System.out.println("节点："+node.host()+":"+node.port());
        }
    }

    /**
     * 列出所有topic
     * @param adminClient
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void listAllTopics(AdminClient adminClient) throws ExecutionException, InterruptedException {
        //可配置选项
        ListTopicsOptions options = new ListTopicsOptions();
        //是否列出内部使用的topics
        options.listInternal(true);
        ListTopicsResult topicsResult = adminClient.listTopics(options);
        //打印topics名称
        Set<String> topicNames = topicsResult.names().get();
        System.out.println(topicNames);
        //打印topics信息
        Collection<TopicListing> topicListings = topicsResult.listings().get();
        System.out.println(topicListings);
    }

    public static void describeTopics(AdminClient adminClient) throws ExecutionException, InterruptedException {
        TopicDescription topicDescription = adminClient.describeTopics(Collections.singleton("lcc")).all().get().get("lcc");
        System.out.println("结果：" + topicDescription);
    }

    /**
     * 创建一个分区数量为3，副本数量为1的主题
     * @param adminClient
     * @param topicName
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void createTopics(AdminClient adminClient, String topicName) throws ExecutionException, InterruptedException {
        NewTopic newTopic = new NewTopic(topicName, 3, (short)1);
        CreateTopicsResult ret = adminClient.createTopics(Arrays.asList(newTopic));
        ret.all().get();
    }

    /**
     * 删除给定topic
     * @param adminClient
     * @param topicName
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void deleteTopics(AdminClient adminClient, String topicName) throws ExecutionException, InterruptedException {
        DeleteTopicsResult ret = adminClient.deleteTopics(Arrays.asList(topicName));
        ret.all().get();
    }

    public static void getMessageListByTopicName(String topicName){

        // 配置 Kafka Consumer
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.149.128:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "your_group"); // 消费组 ID，可任意设置
        props.put("auto.offset.reset", "earliest"); // 确保从最早的数据开始消费（仅首次生效）

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 获取指定 Topic 的所有分区
        List<TopicPartition> partitions = new ArrayList<>(consumer.partitionsFor(topicName).size());
        consumer.partitionsFor(topicName).forEach(partitionInfo ->
                partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
        );

        // 分配分区到 Consumer
        consumer.assign(partitions);

        // 将每个分区的偏移量设置到最开始
        consumer.seekToBeginning(partitions);

        // 拉取数据
        boolean allDataFetched = false; // 标志是否读取完所有分区数据
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions); // 获取每个分区的终止偏移量
        List<ConsumerRecord<String, String>> messageList = new ArrayList<>();
        try {
            while (!allDataFetched) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                allDataFetched = true; // 假设所有数据都已读取完

                for (ConsumerRecord<String, String> record : records) {
//                    System.out.printf("Partition: %d, Offset: %d, Key: %s, Value: %s%n",
//                            record.partition(), record.offset(), record.key(), record.value());
                    messageList.add(record);
                }

                // 检查是否达到每个分区的终止偏移量
                for (TopicPartition partition : partitions) {
                    if (consumer.position(partition) < endOffsets.get(partition)) {
                        allDataFetched = false; // 如果有分区尚未读取完，继续拉取
                    }
                }
            }
            if (messageList.size()==0){
                System.out.println("当前topic暂无数据");
            }
            for (ConsumerRecord<String, String> record : messageList) {
                System.out.printf("Partition: %d, Offset: %d, Key: %s, Value: %s%n",
                        record.partition(), record.offset(), record.key(), record.value());
            }
        } finally {
            consumer.close(); // 关闭 Consumer
        }
    }

    public static void listConsumerGroups(AdminClient adminClient) throws ExecutionException, InterruptedException {
        ListConsumerGroupsResult listConsumerGroupsResult = adminClient.listConsumerGroups();
        KafkaFuture<Collection<ConsumerGroupListing>> all = listConsumerGroupsResult.all();
        KafkaFuture<Collection<ConsumerGroupListing>> valid = listConsumerGroupsResult.valid();

        Collection<ConsumerGroupListing> allList = all.get();

        Collection<ConsumerGroupListing> validList = valid.get();

        System.out.println(allList);
        System.out.println(validList);


        List<String> allGroupIds = new ArrayList<>();

        allList.stream().forEach(consumerGroupListing -> {
            String groupId = consumerGroupListing.groupId();
            allGroupIds.add(groupId);
        });

        List<String> validGroupIds = new ArrayList<>();

        validList.stream().forEach(consumerGroupListing -> {
            String groupId = consumerGroupListing.groupId();
            validGroupIds.add(groupId);
        });

        for (String allGroupId : allGroupIds) {
            System.out.println(allGroupId);
        }


    }

    public static void describeConsumerGroups(AdminClient adminClient) throws ExecutionException, InterruptedException {
        Map<String, ConsumerGroupDescription> map = adminClient.describeConsumerGroups(Collections.singleton("consumer-group-ab67171d-82b0-4ac1-8213-266161c4c3e8")).all().get();
        System.out.println(map);

        ListConsumerGroupOffsetsResult offsetsResult = adminClient.listConsumerGroupOffsets("consumer-group-ab67171d-82b0-4ac1-8213-266161c4c3e8");
        Map<TopicPartition, OffsetAndMetadata> groupOffsets = offsetsResult.partitionsToOffsetAndMetadata().get();
        System.out.println(groupOffsets);
    }

    public static void deleteConsumer(AdminClient adminClient) {
//        adminClient.expireMemberId
    }

    public static void testTopicInfo(AdminClient adminClient) {
        List<TopicInfo> result = new ArrayList<>();

        try {
            //可配置选项
            ListTopicsOptions options = new ListTopicsOptions();
            //是否列出内部使用的topics
            options.listInternal(false);
            ListTopicsResult topicsResult = adminClient.listTopics(options);
            //打印topics名称
            Set<String> topicNames = topicsResult.names().get();
            System.out.println(topicNames);
            Collection<TopicListing> topicListings = topicsResult.listings().get();





            Map<String, TopicDescription> stringTopicDescriptionMap = adminClient.describeTopics(topicNames).all().get();
            for (Map.Entry<String, TopicDescription> stringTopicDescriptionEntry : stringTopicDescriptionMap.entrySet()) {
                String topicName = stringTopicDescriptionEntry.getKey();
                TopicDescription topicDescription = stringTopicDescriptionEntry.getValue();
                int partitionNum = topicDescription.partitions().size();

                TopicInfo topic = new TopicInfo();
                topic.setTopicName(topicName);
                topic.setPartitionNum(partitionNum);


                // 配置 Kafka Consumer
                Properties props = new Properties();
                props.put("bootstrap.servers", "192.168.149.128:9092");
                props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                String groupId = UUID.randomUUID().toString().replace("-","");
                props.put("group.id", groupId); // 消费组 ID，可任意设置
                props.put("auto.offset.reset", "earliest"); // 确保从最早的数据开始消费（仅首次生效）

                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
                List<TopicPartition> partitions = new ArrayList<>();
                consumer.partitionsFor(topicName).forEach(partitionInfo ->
                        partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
                );

                // 分配分区到 Consumer
                consumer.assign(partitions);

                // 将每个分区的偏移量设置到最开始
                consumer.seekToBeginning(partitions);

                // 拉取数据
                boolean allDataFetched = false; // 标志是否读取完所有分区数据
                Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions); // 获取每个分区的终止偏移量
                List<ConsumerRecord<String, String>> messageList = new ArrayList<>();

                Integer messageNum = 0;
                while (!allDataFetched) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    allDataFetched = true; // 假设所有数据都已读取完

                    messageNum += records.count();

                    // 检查是否达到每个分区的终止偏移量
                    for (TopicPartition partition : partitions) {
                        if (consumer.position(partition) < endOffsets.get(partition)) {
                            allDataFetched = false; // 如果有分区尚未读取完，继续拉取
                            break;
                        }
                    }
                }

                topic.setMessageNum(messageNum);

                result.add(topic);
                // 关闭 Consumer
                consumer.close();
                try {
                    adminClient.deleteConsumerGroups(Collections.singleton(groupId)).all().get();
                    System.out.println("删除消费者组成功");
                }catch (Exception e){
                    e.printStackTrace();
                    System.out.println("删除消费者组失败");
                }
            }

            for (TopicInfo topicInfo : result) {
                System.out.println(topicInfo);
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
