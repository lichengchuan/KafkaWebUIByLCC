package com.lcc.kafkaUI.test;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.MalformedURLException;
import java.util.Set;

public class TestJmxAPI {

    public static void main(String[] args){
        String host = "192.168.149.128";
        int port = 9502;
        try{
//            broker_网络请求相关指标(host,port);

//            broker_节点性能指标(host,port);
//            controller_状态(host,port);
    //        分区和副本指标(host,port);
    //        消费者延迟相关指标(host,port);
//            jvm状态();
            topic_info_metric(host,port);
        }catch (Exception e){
            System.out.println("捕获异常");
            e.printStackTrace();
        }
    }

    public static void broker_网络请求相关指标(String host,Integer port) throws Exception{

        MBeanServerConnection connection = connectToKafkaJMX(host, port);

        // 每秒生产请求数
        String produceRequestsMBean = "kafka.network:type=RequestMetrics,name=RequestsPerSec,request=Produce";
        Object produceRequests = getAttribute(connection, produceRequestsMBean, "Count");
        System.out.println("每秒生产请求数: " + produceRequests);

        // 每秒消费者请求数
        String fetchRequestsMBean = "kafka.network:type=RequestMetrics,name=RequestsPerSec,request=FetchConsumer";
        Object fetchRequests = getAttribute(connection, fetchRequestsMBean, "Count");
        System.out.println("每秒消费者请求数: " + fetchRequests);



        // 网络请求队列大小
        String requestQueueMBean = "kafka.network:type=RequestQueue,name=RequestQueueSize";
        Object requestQueueSize = getAttribute(connection, requestQueueMBean, "Count");
        System.out.println("网络请求队列大小: " + requestQueueSize);



    }

    public static void broker_节点性能指标(String host,Integer port) throws Exception{

        MBeanServerConnection connection = connectToKafkaJMX(host, port);

        // 每秒进入的字节数
        String bytesInMBean = "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec";
        Double bytesIn = (Double) getAttribute(connection, bytesInMBean, "MeanRate");
        System.out.println("每秒进入的字节数速率: " + bytesIn +"byte/s");

        Double meanInRate = (Double) getAttribute(connection,bytesInMBean, "MeanRate");
        Double oneMinInRate = (Double) getAttribute(connection,bytesInMBean, "OneMinuteRate");
        Double fiveMinInRate = (Double) getAttribute(connection,bytesInMBean, "FiveMinuteRate");
        Double fifteenMinInRate = (Double) getAttribute(connection,bytesInMBean, "FifteenMinuteRate");

        System.out.println("  每秒进入的字节平均速率: " + meanInRate + " byte/s");
        System.out.println("  最近1分钟进入的字节速率: " + oneMinInRate + " byte/s");
        System.out.println("  最近5分钟进入的字节速率: " + fiveMinInRate + " byte/s");
        System.out.println("  最近15分钟进入的字节速率: " + fifteenMinInRate + " byte/s");


        // 每秒发送的字节数
        String bytesOutMBean = "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec";
//        Double bytesOut = (Double) getAttribute(connection, bytesOutMBean, "MeanRate");
//        System.out.println("每秒发送的字节速率: " + bytesOut +"byte/s");

        Double meanRate = (Double) getAttribute(connection,bytesOutMBean, "MeanRate");
        Double oneMinRate = (Double) getAttribute(connection,bytesOutMBean, "OneMinuteRate");
        Double fiveMinRate = (Double) getAttribute(connection,bytesOutMBean, "FiveMinuteRate");
        Double fifteenMinRate = (Double) getAttribute(connection,bytesOutMBean, "FifteenMinuteRate");

        BigDecimal bd = new BigDecimal(meanRate);
        bd = bd.setScale(3, RoundingMode.HALF_UP);
        meanRate = bd.doubleValue();

        System.out.println("  每秒发送的平均速率: " + meanRate + " byte/s");
        System.out.println("  最近1分钟发送的速率: " + oneMinRate + " byte/s");
        System.out.println("  最近5分钟发送的速率: " + fiveMinRate + " byte/s");
        System.out.println("  最近15分钟发送的速率: " + fifteenMinRate + " byte/s");

        // 每秒写入的消息数
        String messagesInMBean = "kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec";
        Long messagesIn = (Long) getAttribute(connection, messagesInMBean, "Count");
        System.out.println("累计写入消息字节数: " + messagesIn +" bytes");

        String partitionNumMBean = "kafka.server:name=PartitionCount,type=ReplicaManager";
        Integer partitionNum = (Integer) getAttribute(connection, partitionNumMBean, "Value");
        System.out.println(partitionNum);

    }

    public static void topic_info_metric(String host,Integer port) throws Exception{

        MBeanServerConnection connection = connectToKafkaJMX(host, port);

        // 获取特定主题和分区的消费滞后
//        String consumerLagMBean = String.format("kafka.consumer:type=ConsumerFetcherManager,name=RecordsLag,clientId=%s,topic=%s",
//                "consumer-group", topic, partitionId);
//        Double consumerLag = (Double) getAttribute(connection, consumerLagMBean, "Value");
//        System.out.println("消费滞后: " + consumerLag);

        // 获取特定主题的生产速度
        String messagesInMBean = String.format("kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=%s", "lcc");
        Double productionRate = (Double) getAttribute(connection, messagesInMBean, "MeanRate");
        System.out.println("生产速度: " + productionRate + " 消息/秒");
    }



    public static void controller_状态(String host,Integer port) throws Exception{

        MBeanServerConnection connection = connectToKafkaJMX(host, port);

        // 活跃的 Controller 数量
        String activeControllerMBean = "kafka.controller:type=KafkaController,name=ActiveControllerCount";
        Object activeControllerCount = getAttribute(connection, activeControllerMBean, "Value");
        System.out.println("活跃的 Controller 数量: " + activeControllerCount);

        // Leader 选举速率和时间
        String leaderElectionMBean = "kafka.controller:type=ControllerStats,name=LeaderElectionRateAndTimeMs";
        Object leaderElectionRate = getAttribute(connection, leaderElectionMBean, "MeanRate");
        System.out.println("Leader 选举速率和时间: " + leaderElectionRate);
    }

    public static void 分区和副本指标(String host,Integer port) throws Exception{

        MBeanServerConnection connection = connectToKafkaJMX(host, port);

        // 未同步的分区数
        String underReplicatedPartitionsMBean = "kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions";
        Object underReplicatedPartitions = getAttribute(connection, underReplicatedPartitionsMBean, "Value");
        System.out.println("未同步的分区数: " + underReplicatedPartitions);

        // 每秒 ISR 收缩次数
        String isrShrinksMBean = "kafka.server:type=ReplicaManager,name=IsrShrinksPerSec";
        Object isrShrinksPerSec = getAttribute(connection, isrShrinksMBean, "MeanRate");
        System.out.println("每秒 ISR 收缩次数: " + isrShrinksPerSec);

        // 分区总数
        String partitionCountMBean = "kafka.server:type=ReplicaManager,name=PartitionCount";
        Object partitionCount = getAttribute(connection, partitionCountMBean, "Value");
        System.out.println("分区总数: " + partitionCount);
    }

    public static void 消费者延迟相关指标(String host,Integer port) throws Exception{

        MBeanServerConnection connection = connectToKafkaJMX(host, port);

        // 最大 Lag
        String recordsLagMaxMBean = "kafka.consumer:type=consumer-fetch-manager-metrics,client-id=consumer-console-consumer-38288-1,name=records-lag-max";
        Object recordsLagMax = getAttribute(connection, recordsLagMaxMBean, "Value");
        System.out.println("最大 Lag: " + recordsLagMax);

        // Fetch 请求速率
        String fetchRateMBean = "kafka.consumer:type=consumer-fetch-manager-metrics,client-id=consumer-consumer-group-8b2551cd-754b-4104-b812-bc5f29d8df47-4,name=fetch-rate";
        Object fetchRate = getAttribute(connection, fetchRateMBean, "MeanRate");
        System.out.println("Fetch 请求速率: " + fetchRate);
    }


    public static void jvm状态() throws Exception{
        String host = "192.168.149.128"; // 替换为你的Kafka Broker主机名或IP地址
        Integer port = 9504; // 替换为实际的JMX端口


        JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + host + ":" + port + "/jmxrmi");
        JMXConnector connector = JMXConnectorFactory.connect(url);
        MBeanServerConnection connection = connector.getMBeanServerConnection();

        // 获取JVM内存使用情况
        ObjectName memoryMBean = new ObjectName("java.lang:type=Memory");
        CompositeData memInfo = (CompositeData) connection.getAttribute(memoryMBean, "HeapMemoryUsage");
        double used = (double) (Long)memInfo.get("used");
        double max = (double) (Long)memInfo.get("committed");
        double a= (used/max);
        System.out.printf("JVM 堆内存使用率: %.2f%%\n" , a*100);

        // 获取非堆内存使用情况
        CompositeData nonHeapMemInfo = (CompositeData) connection.getAttribute(memoryMBean, "NonHeapMemoryUsage");
        double nonHeapUsed = (double) (Long)nonHeapMemInfo.get("used");
        double nonHeapMax = (double) (Long)nonHeapMemInfo.get("committed");
        System.out.printf("JVM 堆外内存使用率: %.2f%%\n", (nonHeapUsed / nonHeapMax) * 100);

        // 获取JVM线程信息
        ObjectName threadMBean = new ObjectName("java.lang:type=Threading");
        Integer threadCount = (Integer) connection.getAttribute(threadMBean, "ThreadCount");
        System.out.println("JVM 线程数: " + threadCount);

        // 获取操作系统信息
        ObjectName osMBean = new ObjectName("java.lang:type=OperatingSystem");

        // 系统负载平均值（1分钟、5分钟、15分钟）
//        double osLoadAverage = (double) connection.getAttribute(osMBean, "SystemCpuLoad");
//        double systemLoadAverage = (Double) connection.getAttribute(osMBean, "SystemLoadAverage");
//        double[] loadAverages = (double[]) osLoadAverage.get("loadAverage");

        // 可用处理器数量
        Integer availableProcessors = (Integer) connection.getAttribute(osMBean, "AvailableProcessors");

        // 系统CPU使用率（仅在Java 9+中可用）
        Double systemCpuLoad = (Double) connection.getAttribute(osMBean, "SystemCpuLoad");

        // 进程CPU使用率（仅在Java 9+中可用）
        Double processCpuLoad = (Double) connection.getAttribute(osMBean, "ProcessCpuLoad");

        // 操作系统名称、版本、架构
        String osName = (String) connection.getAttribute(osMBean, "Name");
        String osVersion = (String) connection.getAttribute(osMBean, "Version");
        String osArch = (String) connection.getAttribute(osMBean, "Arch");

        // 打印操作系统信息
        System.out.println("操作系统名称: " + osName);
        System.out.println("操作系统版本: " + osVersion);
        System.out.println("操作系统架构: " + osArch);
        System.out.println("可用处理器数量: " + availableProcessors);

        // 如果获取到的系统负载平均值不为null，则打印出来
//        if (systemLoadAverage != -1.0) {
//            System.out.printf("系统负载平均值 (1分钟): %.2f\n", loadAverages[0]);
//            System.out.printf("系统负载平均值 (5分钟): %.2f\n", loadAverages[1]);
//            System.out.printf("系统负载平均值 (15分钟): %.2f\n", loadAverages[2]);
//        } else {
//            System.out.println("无法获取系统负载平均值。");
//        }

        // 如果是Java 9及以上版本，打印CPU使用率
        if (systemCpuLoad != null && processCpuLoad != null) {
            System.out.printf("系统CPU使用率: %.2f%%\n", systemCpuLoad * 100);
            System.out.printf("进程CPU使用率: %.2f%%\n", processCpuLoad * 100);
        } else {
            System.out.println("当前JVM版本不支持获取CPU使用率。");
        }

        // 获取总物理内存大小（字节）
        Long totalPhysicalMemorySize = (Long) connection.getAttribute(osMBean, "TotalPhysicalMemorySize");

        // 获取空闲物理内存大小（字节）
        Long freePhysicalMemorySize = (Long) connection.getAttribute(osMBean, "FreePhysicalMemorySize");

        // 计算已用物理内存
        Long usedPhysicalMemorySize = totalPhysicalMemorySize - freePhysicalMemorySize;

        // 打印内存信息
        System.out.printf("总物理内存: %.2f GB\n", totalPhysicalMemorySize / (1024.0 * 1024 * 1024));
        System.out.printf("已用物理内存: %.2f GB\n", usedPhysicalMemorySize / (1024.0 * 1024 * 1024));
        System.out.printf("空闲物理内存: %.2f GB\n", freePhysicalMemorySize / (1024.0 * 1024 * 1024));


        connector.close();

    }


    public static MBeanServerConnection connectToKafkaJMX(String host, int port) throws Exception {
        // JMX 服务 URL
        String jmxUrl = String.format("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi", host, port);
        JMXServiceURL serviceURL = new JMXServiceURL(jmxUrl);

        // 创建 JMX 连接
        JMXConnector jmxConnector = JMXConnectorFactory.connect(serviceURL, null);
        return jmxConnector.getMBeanServerConnection();
    }

    public static Object getAttribute(MBeanServerConnection connection, String objectName, String attribute) throws Exception {
        ObjectName mbeanName = new ObjectName(objectName);
        return connection.getAttribute(mbeanName,attribute);
    }



}
