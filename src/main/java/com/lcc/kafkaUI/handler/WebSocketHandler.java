package com.lcc.kafkaUI.handler;

import com.lcc.kafkaUI.config.AdminClientConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class WebSocketHandler extends TextWebSocketHandler {

    @Autowired
    private AdminClient adminClient;
    @Autowired
    private AdminClientConfig adminClientConfig;

    // 创建线程池来管理消费者线程
    private final ExecutorService executorService = Executors.newFixedThreadPool(10);

    // 处理接收到的消息
    @Override
    public void handleTextMessage(WebSocketSession session, TextMessage message) {
        // 打印接收到的消息
        System.out.println("收到消息: " + message.getPayload());
        // 向客户端发送消息
        try {
            session.sendMessage(new TextMessage(message.getPayload()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession webSocketSession) throws Exception {
        super.afterConnectionEstablished(webSocketSession);
        System.out.println("连接成功");

        String path = webSocketSession.getUri().getPath();

        String topicName = path.substring(path.lastIndexOf("/")+1);
        System.out.println("订阅的 Topic: " + topicName);

        // 动态生成一个唯一的 group.id
        String groupId = "consumer-group-" + UUID.randomUUID();
        Properties properties = new Properties();
        properties.put("bootstrap.servers", adminClientConfig.getBootstrapServers());
        properties.put("group.id", groupId); // 每个 WebSocket 客户端使用一个独立的消费组 ID
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topicName));
        // 启动新的线程消费消息
        executorService.submit(() -> {
            try {
                while (webSocketSession.isOpen()) {

                    // 每次拉取 Kafka 数据时增加合适的超时时间
                    consumer.poll(Duration.ofMillis(100)).forEach(record -> {
                        try {
                            // 如果 WebSocket 连接有效，发送消息给前端
                            if (webSocketSession.isOpen()) {
                                System.out.println("成立");
                                webSocketSession.sendMessage(new TextMessage(record.value()));
                            }else {
                                System.out.println("关闭");
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                System.out.println("consumer关闭");
                consumer.close();
                try {
                    adminClient.deleteConsumerGroups(Collections.singleton(groupId)).all().get();
                    System.out.println("删除消费者组成功");
                }catch (Exception e){
                    e.printStackTrace();
                    System.out.println("删除消费者组失败");
                }
            }
        });
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
        super.afterConnectionClosed(session, status);
        System.out.println("连接关闭");
    }
}
