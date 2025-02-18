package com.lcc.kafkaUI.config;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Configuration
@Data
public class AdminClientConfig {

    public static String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    @Value("${kafka.bootstrap.servers:172.24.32.232:9092}")
    private String bootstrapServers;

    @Autowired(required = false)
    private ZooKeeper zooKeeper;

    @Bean
    public AdminClient adminClient() throws Exception {
        if(zooKeeper != null){
            initKafkaNodeInfoByZk();
        }
        Properties properties = new Properties();
        properties.put(org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.put("enable.auto.commit", "false"); // 禁止自动提交偏移量
        AdminClient adminClient = AdminClient.create(properties);
        return adminClient;
    }

    private void initKafkaNodeInfoByZk() throws KeeperException, InterruptedException, UnsupportedEncodingException {
        // 获取所有Broker ID
        List<String> brokerIds = zooKeeper.getChildren("/brokers/ids", false);

        if (brokerIds.isEmpty()){
            throw new RuntimeException("当前zookeeper没有kafka连接");
        }

        String brokerId = brokerIds.get(0);
        byte[] data = zooKeeper.getData("/brokers/ids/" + brokerId, false, new Stat());
        String brokerInfoStr = new String(data, "UTF-8");
        Map map = JSON.parseObject(brokerInfoStr, Map.class);
        String host = (String) map.get("host");
        Integer port = (Integer) map.get("port");
        bootstrapServers = host + ":" + port;
    }


}
