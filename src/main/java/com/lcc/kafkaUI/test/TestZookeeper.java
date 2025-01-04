package com.lcc.kafkaUI.test;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.List;

public class TestZookeeper {
    private static final String ZOOKEEPER_HOST = "192.168.149.128:2181"; // 替换为你的ZooKeeper地址
    private static final int SESSION_TIMEOUT = 5000; // ZooKeeper会话超时时间

    public static void main(String[] args) throws Exception {
        // 连接到ZooKeeper
        ZooKeeper zk = new ZooKeeper(ZOOKEEPER_HOST, SESSION_TIMEOUT, event -> {});

        try {
            // 获取所有Broker ID
            List<String> brokerIds = zk.getChildren("/brokers/ids", false);

            System.out.println("当前活跃的Broker列表:");
            for (String brokerId : brokerIds) {
                byte[] data = zk.getData("/brokers/ids/" + brokerId, false, new Stat());
                String brokerInfo = new String(data, "UTF-8");
                System.out.printf("Broker ID: %s, Info: %s\n", brokerId, brokerInfo);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            zk.close();
        }
    }
}
