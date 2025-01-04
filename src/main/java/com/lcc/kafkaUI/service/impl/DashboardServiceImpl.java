package com.lcc.kafkaUI.service.impl;

import com.alibaba.fastjson.JSON;
import com.lcc.kafkaUI.common.utils.DateUtils;
import com.lcc.kafkaUI.config.AdminClientConfig;
import com.lcc.kafkaUI.entity.Topic;
import com.lcc.kafkaUI.service.DashboardService;
import com.lcc.kafkaUI.vo.dashborad.BrokerInfo;
import com.lcc.kafkaUI.vo.dashborad.ClusterInfo;
import com.lcc.kafkaUI.vo.dashborad.TopicInfo;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.*;


@Service
public class DashboardServiceImpl implements DashboardService {

    @Autowired
    private AdminClient adminClient;

    @Autowired
    private ZooKeeper zooKeeper;

    private static final int SCALE = 3; // 保留的小数位数


    @Override
    public ClusterInfo getClusterInfo() {

        ClusterInfo result = new ClusterInfo();

        try {
            DescribeClusterResult clusterInfo = adminClient.describeCluster();

            //获取集群ID
            String clusterID = clusterInfo.clusterId().get();
            result.setId(clusterID);

            Node controller = clusterInfo.controller().get();
            result.setController(controller.host()+":"+controller.port());

            int brokerCount = clusterInfo.nodes().get().size();
            result.setBrokerCount(brokerCount);

            int topicCount = adminClient.listTopics().listings().get().size();
            result.setTopicCount(topicCount);

            return result;

        }catch (Exception e){
            return null;
        }
    }

    @Override
    public List<BrokerInfo> getBrokerInfo() {

        List<BrokerInfo> result = new ArrayList<>();

        try {
            // 获取所有Broker ID
            List<String> brokerIds = zooKeeper.getChildren("/brokers/ids", false);

            System.out.println("当前活跃的Broker列表:");
            for (String brokerId : brokerIds) {
                BrokerInfo brokerInfo = new BrokerInfo();
                byte[] data = zooKeeper.getData("/brokers/ids/" + brokerId, false, new Stat());
                String brokerInfoStr = new String(data, "UTF-8");
                System.out.printf("Broker ID: %s, Info: %s\n", brokerId, brokerInfoStr);
                Map map = JSON.parseObject(brokerInfoStr, Map.class);
                Integer jmx_port = (Integer) map.get("jmx_port");
                String host = (String) map.get("host");
                Integer port = (Integer) map.get("port");
                brokerInfo.setId(brokerId);
                brokerInfo.setHost(host);
                brokerInfo.setPort(port);
                brokerInfo.setJmxPort(jmx_port);
                result.add(brokerInfo);
            }

            //2.连接JMX获取一系列指标
            for (BrokerInfo brokerInfo : result) {
                JMXConnector jmxConnector = connectToKafkaJMX(brokerInfo.getHost(), brokerInfo.getJmxPort());
                MBeanServerConnection mBeanServerConnection = jmxConnector.getMBeanServerConnection();

                // 进入的字节数速率
                String bytesInMBean = "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec";


                Double meanInRate = round(getAttribute(mBeanServerConnection, bytesInMBean, "MeanRate"));
                Double oneMinInRate = round(getAttribute(mBeanServerConnection, bytesInMBean, "OneMinuteRate"));
                Double fiveMinInRate = round(getAttribute(mBeanServerConnection, bytesInMBean, "FiveMinuteRate"));
                Double fifteenMinInRate = round(getAttribute(mBeanServerConnection, bytesInMBean, "FifteenMinuteRate"));

                brokerInfo.setBytesInRate(meanInRate);
                brokerInfo.setBytesInOneMinRate(oneMinInRate);
                brokerInfo.setBytesIn5MinRate(fiveMinInRate);
                brokerInfo.setBytesIn15MinRate(fifteenMinInRate);
                System.out.println("brokerId:"+brokerInfo.getId()+": 每秒进入的字节平均速率: " + meanInRate + " byte/s");
                System.out.println("brokerId:"+brokerInfo.getId()+": 最近1分钟进入的字节速率: " + oneMinInRate + " byte/s");
                System.out.println("brokerId:"+brokerInfo.getId()+": 最近5分钟进入的字节速率: " + fiveMinInRate + " byte/s");
                System.out.println("brokerId:"+brokerInfo.getId()+": 最近15分钟进入的字节速率: " + fifteenMinInRate + " byte/s");


                // 发送的字节数速率
                String bytesOutMBean = "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec";

                Double meanOutRate = round(getAttribute(mBeanServerConnection, bytesOutMBean, "MeanRate"));
                Double oneMinOutRate = round(getAttribute(mBeanServerConnection, bytesOutMBean, "OneMinuteRate"));
                Double fiveMinOutRate = round(getAttribute(mBeanServerConnection, bytesOutMBean, "FiveMinuteRate"));
                Double fifteenMinOutRate = round(getAttribute(mBeanServerConnection, bytesOutMBean, "FifteenMinuteRate"));

                brokerInfo.setBytesOutRate(meanOutRate);
                brokerInfo.setBytesOutOneMinRate(oneMinOutRate);
                brokerInfo.setBytesOut5MinRate(fiveMinOutRate);
                brokerInfo.setBytesOut15MinRate(fifteenMinOutRate);

                System.out.println("brokerId:"+brokerInfo.getId()+": 每秒发送的平均速率: " + meanOutRate + " byte/s");
                System.out.println("brokerId:"+brokerInfo.getId()+": 最近1分钟发送的速率: " + oneMinOutRate + " byte/s");
                System.out.println("brokerId:"+brokerInfo.getId()+": 最近5分钟发送的速率: " + fiveMinOutRate + " byte/s");
                System.out.println("brokerId:"+brokerInfo.getId()+": 最近15分钟发送的速率: " + fifteenMinOutRate + " byte/s");


                // 累计写入消息字节数
                String messagesInMBean = "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec";
                Long messagesIn = (Long) getAttribute(mBeanServerConnection, messagesInMBean, "Count");
                brokerInfo.setMessagesInBytes(messagesIn);
                System.out.println("brokerId:"+brokerInfo.getId()+"累计写入消息字节数: " + messagesIn +" byte");

                //分区数
                String partitionNumMBean = "kafka.server:name=PartitionCount,type=ReplicaManager";
                Integer partitionNum = (Integer) getAttribute(mBeanServerConnection, partitionNumMBean, "Value");
                brokerInfo.setPartitionNum(partitionNum);
                System.out.println("brokerId:"+brokerInfo.getId()+"分区数："+partitionNum);

                // 获取JVM内存使用情况
                String jvmMemoryMBean = "java.lang:type=Memory";
                CompositeData memInfo = (CompositeData) getAttribute(mBeanServerConnection, jvmMemoryMBean,"HeapMemoryUsage");
                double used = (double) (Long)memInfo.get("used");
                double committed = (double) (Long)memInfo.get("committed");
                double a= (used/committed);
                brokerInfo.setJvmUsedMemory(a);
                System.out.printf("brokerId:"+brokerInfo.getId()+"JVM 堆内存使用率: %.2f%%\n" , a*100);

                // 获取JVM线程信息
                String jvmThreadMBean = "java.lang:type=Threading";
                Integer threadCount = (Integer) getAttribute(mBeanServerConnection,jvmThreadMBean,"ThreadCount");
                brokerInfo.setJvmThreads(threadCount);
                System.out.println("brokerId:"+brokerInfo.getId()+"JVM 线程数: " + threadCount);

                // 获取操作系统信息
                String osMBean = "java.lang:type=OperatingSystem";

                // 系统CPU使用率（仅在Java 9+中可用）
                Double systemCpuLoad = (Double) getAttribute(mBeanServerConnection,osMBean, "SystemCpuLoad");
                brokerInfo.setSystemCpuLoad(systemCpuLoad);

                // 进程CPU使用率（仅在Java 9+中可用）
                Double processCpuLoad = (Double) getAttribute(mBeanServerConnection,osMBean, "ProcessCpuLoad");
                brokerInfo.setProcessCpuLoad(processCpuLoad);

                System.out.printf("brokerId:"+brokerInfo.getId()+"系统CPU使用率: %.2f%%\n", systemCpuLoad * 100);
                System.out.printf("brokerId:"+brokerInfo.getId()+"进程CPU使用率: %.2f%%\n", processCpuLoad * 100);


                // 获取总物理内存大小（字节）
                Long totalPhysicalMemorySize = (Long) getAttribute(mBeanServerConnection,osMBean, "TotalPhysicalMemorySize");
                brokerInfo.setTotalPhysicalMemorySize(totalPhysicalMemorySize);

                // 获取空闲物理内存大小（字节）
                Long freePhysicalMemorySize = (Long) getAttribute(mBeanServerConnection,osMBean, "FreePhysicalMemorySize");
                brokerInfo.setFreePhysicalMemorySize(freePhysicalMemorySize);

                // 计算已用物理内存
                Long usedPhysicalMemorySize = totalPhysicalMemorySize - freePhysicalMemorySize;
                brokerInfo.setUsedPhysicalMemorySize(usedPhysicalMemorySize);

                // 打印内存信息
                System.out.printf("总物理内存: %.2f GB\n", totalPhysicalMemorySize / (1024.0 * 1024 * 1024));
                System.out.printf("已用物理内存: %.2f GB\n", usedPhysicalMemorySize / (1024.0 * 1024 * 1024));
                System.out.printf("空闲物理内存: %.2f GB\n", freePhysicalMemorySize / (1024.0 * 1024 * 1024));


                jmxConnector.close();
            }
            return result;
        }catch (Exception e){
            e.printStackTrace();
            return result;
        }


    }

    @Override
    public Map getMessageInfoWithWeek() {

        Map<String,Integer> dataMap = new HashMap<>();

        try {
            //可配置选项
            ListTopicsOptions options = new ListTopicsOptions();
            //是否列出内部使用的topics
            options.listInternal(false);
            ListTopicsResult topicsResult = adminClient.listTopics(options);
            //打印topics名称
            Set<String> topicNames = topicsResult.names().get();
            System.out.println(topicNames);

            // 配置 Kafka Consumer
            Properties props = new Properties();
            props.put("bootstrap.servers", AdminClientConfig.host+":"+ AdminClientConfig.port);
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            String groupId = "dashboard";
            props.put("group.id", groupId); // 消费组 ID，可任意设置
            props.put("auto.offset.reset", "earliest"); // 确保从最早的数据开始消费（仅首次生效）

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            List<TopicPartition> partitions = new ArrayList<>();
            for (String topicName : topicNames) {
                consumer.partitionsFor(topicName).forEach(partitionInfo ->
                        partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
                );
            }

            // 分配分区到 Consumer
            consumer.assign(partitions);

            // 将每个分区的偏移量设置到最开始
            consumer.seekToBeginning(partitions);

            // 拉取数据
            boolean allDataFetched = false; // 标志是否读取完所有分区数据
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions); // 获取每个分区的终止偏移量
            List<ConsumerRecord<String, String>> messageList = new ArrayList<>();

            // 计算七天前的时间戳，并将其转换为北京时间
            Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("Asia/Shanghai"));

            for (int i = 0; i < 7; i++) {
                Date currentDate = calendar.getTime();
                String dateKey = DateUtils.simpleDateFormat2.format(currentDate);
                dataMap.put(dateKey, 0); // 初始化计数为0

                calendar.add(Calendar.DAY_OF_MONTH, -1); // 回溯一天
            }
            calendar.setTimeInMillis(System.currentTimeMillis());
            calendar.add(Calendar.DAY_OF_MONTH, -7);
            Date sevenDaysAgo = calendar.getTime();



            try {
                while (!allDataFetched) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    allDataFetched = true; // 假设所有数据都已读取完

                    for (ConsumerRecord<String, String> record : records) {
                        // 将记录的时间戳转换为北京时间
                        calendar.setTimeInMillis(record.timestamp());
                        Date recordDate = calendar.getTime();

                        // 只处理过去七天内的消息
                        if (!recordDate.before(sevenDaysAgo)) {
                            String dateKey = DateUtils.simpleDateFormat2.format(recordDate);
                            dataMap.put(dateKey,dataMap.get(dateKey)+1);
                        }
                    }

                    // 检查是否达到每个分区的终止偏移量
                    for (TopicPartition partition : partitions) {
                        if (consumer.position(partition) < endOffsets.get(partition)) {
                            allDataFetched = false; // 如果有分区尚未读取完，继续拉取
                            break;
                        }
                    }
                }



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
            }

        }catch (Exception e){
            e.printStackTrace();
        }

        return dataMap;
    }

    @Override
    public Map getMessageInfoWithMonth() {

        Map<String,Integer> dataMap = new HashMap<>();

        try {
            //可配置选项
            ListTopicsOptions options = new ListTopicsOptions();
            //是否列出内部使用的topics
            options.listInternal(false);
            ListTopicsResult topicsResult = adminClient.listTopics(options);
            //打印topics名称
            Set<String> topicNames = topicsResult.names().get();
            System.out.println(topicNames);

            // 配置 Kafka Consumer
            Properties props = new Properties();
            props.put("bootstrap.servers", AdminClientConfig.host+":"+AdminClientConfig.port);
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            String groupId = "dashboard";
            props.put("group.id", groupId); // 消费组 ID，可任意设置
            props.put("auto.offset.reset", "earliest"); // 确保从最早的数据开始消费（仅首次生效）

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            List<TopicPartition> partitions = new ArrayList<>();
            for (String topicName : topicNames) {
                consumer.partitionsFor(topicName).forEach(partitionInfo ->
                        partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
                );
            }

            // 分配分区到 Consumer
            consumer.assign(partitions);

            // 将每个分区的偏移量设置到最开始
            consumer.seekToBeginning(partitions);

            // 拉取数据
            boolean allDataFetched = false; // 标志是否读取完所有分区数据
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions); // 获取每个分区的终止偏移量
            List<ConsumerRecord<String, String>> messageList = new ArrayList<>();

            // 计算七天前的时间戳，并将其转换为北京时间
            Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("Asia/Shanghai"));

            for (int i = 0; i < 30; i++) {
                Date currentDate = calendar.getTime();
                String dateKey = DateUtils.simpleDateFormat2.format(currentDate);
                dataMap.put(dateKey, 0); // 初始化计数为0

                calendar.add(Calendar.DAY_OF_MONTH, -1); // 回溯一天
            }
            calendar.setTimeInMillis(System.currentTimeMillis());
            calendar.add(Calendar.DAY_OF_MONTH, -30);
            Date monthAgo = calendar.getTime();



            try {
                while (!allDataFetched) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    allDataFetched = true; // 假设所有数据都已读取完

                    for (ConsumerRecord<String, String> record : records) {
                        // 将记录的时间戳转换为北京时间
                        calendar.setTimeInMillis(record.timestamp());
                        Date recordDate = calendar.getTime();

                        // 只处理过去七天内的消息
                        if (!recordDate.before(monthAgo)) {
                            String dateKey = DateUtils.simpleDateFormat2.format(recordDate);
                            dataMap.put(dateKey,dataMap.get(dateKey)+1);
                        }
                    }

                    // 检查是否达到每个分区的终止偏移量
                    for (TopicPartition partition : partitions) {
                        if (consumer.position(partition) < endOffsets.get(partition)) {
                            allDataFetched = false; // 如果有分区尚未读取完，继续拉取
                            break;
                        }
                    }
                }



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
            }

        }catch (Exception e){
            e.printStackTrace();
        }

        return dataMap;
    }

    @Override
    public List<TopicInfo> getTopicInfo() {

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
                props.put("bootstrap.servers", AdminClientConfig.host+":"+AdminClientConfig.port);
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

        return result;
    }

    private JMXConnector connectToKafkaJMX(String host, int port) throws Exception {
        System.out.println("jmx:"+host+":"+port);
        // JMX 服务 URL
        String jmxUrl = String.format("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi", host, port);
        JMXServiceURL serviceURL = new JMXServiceURL(jmxUrl);

        // 创建 JMX 连接
        JMXConnector jmxConnector = JMXConnectorFactory.connect(serviceURL, null);
        return jmxConnector;
    }

    public Object getAttribute(MBeanServerConnection connection, String objectName, String attribute) throws Exception {
        ObjectName mbeanName = new ObjectName(objectName);
        return connection.getAttribute(mbeanName,attribute);
    }

    private Double round(Object value) {
        if (value instanceof Number) {
            BigDecimal bd = new BigDecimal(((Number) value).doubleValue());
            bd = bd.setScale(SCALE, RoundingMode.HALF_UP);
            return bd.doubleValue();
        }
        return null;
    }
}
