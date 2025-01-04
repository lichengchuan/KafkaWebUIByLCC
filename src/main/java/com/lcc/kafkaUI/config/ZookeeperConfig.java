package com.lcc.kafkaUI.config;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class ZookeeperConfig {

    @Value("${zookeeper.host:localhost}")
    private String ZOOKEEPER_HOST; // 替换为你的ZooKeeper地址
    @Value("${zookeeper.port:2181}")
    private Integer ZOOKEEPER_PORT; // 替换为你的ZooKeeper地址
    @Value("${zookeeper.session_timeout:25000}")
    private int SESSION_TIMEOUT; // ZooKeeper会话超时时间

    @Bean
    public ZooKeeper zooKeeper() throws Exception{
        // 连接到ZooKeeper
        ZooKeeper zkCli = new ZooKeeper(ZOOKEEPER_HOST+":"+ZOOKEEPER_PORT, SESSION_TIMEOUT, event -> {
            System.out.println("zookeeper连接成功");
        });
        return zkCli;
    }
}
