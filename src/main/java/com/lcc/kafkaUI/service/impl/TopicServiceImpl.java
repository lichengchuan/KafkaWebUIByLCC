package com.lcc.kafkaUI.service.impl;

import com.lcc.kafkaUI.common.utils.DataTransferUtil;
import com.lcc.kafkaUI.common.utils.DateUtils;
import com.lcc.kafkaUI.config.AdminClientConfig;
import com.lcc.kafkaUI.dto.topic.TopicCreateDto;
import com.lcc.kafkaUI.entity.ClusterNode;
import com.lcc.kafkaUI.entity.Message;
import com.lcc.kafkaUI.entity.Partition;
import com.lcc.kafkaUI.entity.Topic;
import com.lcc.kafkaUI.service.TopicService;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class TopicServiceImpl implements TopicService {

    @Autowired
    private AdminClient adminClient;

    @Autowired
    private AdminClientConfig adminClientConfig;
    @Override
    public List<Topic> getTopicList(boolean isInternal) {
        List<Topic> result = new ArrayList<>();
        try {
            //可配置选项
            ListTopicsOptions options = new ListTopicsOptions();
            //是否列出内部使用的topics
            options.listInternal(isInternal);
            ListTopicsResult topicsResult = adminClient.listTopics(options);
            //打印topics名称
            Set<String> topicNames = topicsResult.names().get();
            System.out.println(topicNames);
            Collection<TopicListing> topicListings = topicsResult.listings().get();

            for (TopicListing topicListing : topicListings) {
                Topic topic = new Topic();
                topic.setName(topicListing.name());
                topic.setIsInternal(topicListing.isInternal());
                result.add(topic);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public Map<String,Object> getMessageByTopic(String topicName, String startDate, String endDate,int pageNumber, int pageSize) {

        if (!isExistTopic(topicName)){
            return null;
        }

        Map<String,Object> resultMap = new HashMap<>();
        resultMap.put("total",0);



        List<Message> result = new ArrayList<>();

        // 配置 Kafka Consumer
        Properties props = new Properties();
        props.put("bootstrap.servers",adminClientConfig.getBootstrapServers());
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 动态生成一个唯一的 group.id  这个Group肯定是要删除的
        String groupId = "consumer-group-" + UUID.randomUUID();
        props.put("group.id", groupId); // 消费组 ID，可任意设置
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
                    Message message = new Message();
                    message.setName(record.topic());
                    message.setPartition(record.partition());
                    message.setOffset(record.offset());
                    message.setTimestamp(record.timestamp());
                    message.setDate(new Date(record.timestamp()));
                    message.setTimestampType(String.valueOf(record.timestampType()));
                    message.setSerializedKeySize(record.serializedKeySize());
                    message.setSerializedValueSize(record.serializedValueSize());
                    message.setHeaders(String.valueOf(record.headers()));
                    message.setKey(record.key());
                    message.setValue(record.value());

                    result.add(message);
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
            // 处理日期过滤
            Date start = null;
            if (startDate!=null && !startDate.equals("")){
                try {
                    start = DateUtils.simpleDateFormat1.parse(startDate);
                } catch (ParseException e) {
                    e.printStackTrace();
                    return null;
                }
            }
            Date end = null;
            if (endDate!=null && !endDate.equals("")){
                try {
                    end = DateUtils.simpleDateFormat1.parse(endDate);
                } catch (ParseException e) {
                    e.printStackTrace();
                    return null;
                }
            }
            Date finalStart = start;
            Date finalEnd = end;
            result = result.stream().filter(message -> {
                        boolean s = true;
                        boolean e = true;
                        if (finalStart != null) {
                            s = message.getDate().compareTo(finalStart) >= 0;
                        }
                        if (finalEnd != null) {
                            e = message.getDate().compareTo(finalEnd) <= 0;
                        }
                        return (s && e);
                    })
                    .sorted(Comparator.comparingLong(Message::getTimestamp)) // 根据 timestamp 排序
                    .collect(Collectors.toList());

            resultMap.put("total",result.size());


            // 分页逻辑：skip + limit
            int skipCount = (pageNumber - 1) * pageSize;
            result = result.stream()
                    .skip(skipCount)
                    .limit(pageSize)
                    .collect(Collectors.toList());
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            // 关闭 Consumer
            consumer.close();

            try {
                adminClient.deleteConsumerGroups(Collections.singleton(groupId)).all().get();
                System.out.println("删除消费者组成功");
            }catch (Exception e){
                e.printStackTrace();
                System.out.println("删除消费者组失败");
            }


            resultMap.put("data",result);
            return resultMap;
        }
    }

    @Override
    public String createTopic(TopicCreateDto topicCreateDto) {
        try {

            if (isExistTopic(topicCreateDto.getTopicName())){
                return "该topic已存在，请勿重复创建";
            }
            NewTopic newTopic = new NewTopic(topicCreateDto.getTopicName(), topicCreateDto.getPartitions(), topicCreateDto.getReplication());
            CreateTopicsResult ret = adminClient.createTopics(Arrays.asList(newTopic));
            ret.all().get();
            return "创建成功";
        } catch (Exception e) {
            e.printStackTrace();
            return "创建失败："+e.getMessage();
        }

    }

    @Override
    public String deleteTopics(List<String> topicNameList) {

        if (topicNameList.size()<=0)return "操作成功";
        try {
            DeleteTopicsResult ret = adminClient.deleteTopics(topicNameList);
            ret.all().get();
            return "操作成功";
        }catch (Exception e) {
            return "操作失败";
        }

    }

    @Override
    public List<Partition> getTopicInfo(String topicName) {

        List<Partition> result = new ArrayList<>();

        if (!isExistTopic(topicName)){
            return null;
        }

        try {
            TopicDescription topicDescription = adminClient.describeTopics(Collections.singleton(topicName)).all().get().get(topicName);

            List<TopicPartitionInfo> partitions = topicDescription.partitions();

            for (TopicPartitionInfo partitionInfo : partitions) {
                Partition partition = new Partition();
                partition.setPartitionNum(partitionInfo.partition());

                ClusterNode leader = DataTransferUtil.nodeToClusterNode(partitionInfo.leader());
                partition.setLeader(leader);

                List<ClusterNode> replicasList = DataTransferUtil.nodeListToClusterNode(partitionInfo.replicas());
                partition.setReplicas(replicasList);

                List<ClusterNode> isrList = DataTransferUtil.nodeListToClusterNode(partitionInfo.isr());
                partition.setIsr(isrList);
                result.add(partition);
            }
            return result;
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }

    }

    @Override
    public String increasePartitions(Integer num, String topicName) {
        if (!isExistTopic(topicName)){
            return "topic不存在";
        }
        try {
            adminClient.createPartitions(Collections.singletonMap(topicName, NewPartitions.increaseTo(num))).all().get();
            return "操作成功";
        }catch (Exception e){
            e.printStackTrace();
            return "操作失败";
        }

    }


    public boolean isExistTopic(String topicName){
        //可配置选项
        ListTopicsOptions options = new ListTopicsOptions();
        //是否列出内部使用的topics
        options.listInternal(true);
        ListTopicsResult topicsResult = adminClient.listTopics(options);
        //打印topics名称
        Set<String> topicNames = null;
        try {
            topicNames = topicsResult.names().get();
            return topicNames.contains(topicName);
        } catch (Exception e) {
            return true;
        }
    }


}
