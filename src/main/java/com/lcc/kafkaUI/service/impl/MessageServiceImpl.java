package com.lcc.kafkaUI.service.impl;

import com.lcc.kafkaUI.config.AdminClientConfig;
import com.lcc.kafkaUI.dto.message.ProduceMockMessageDto;
import com.lcc.kafkaUI.service.MessageService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Properties;


@Service
public class MessageServiceImpl implements MessageService {

    @Autowired
    private AdminClientConfig adminClientConfig;

    @Override
    public String sendMessage(ProduceMockMessageDto produceMockMessageDto) {
        // 配置KafkaProducer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,adminClientConfig.getBootstrapServers()); // 设置Kafka服务器
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // 设置key的序列化方式
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // 设置value的序列化方式

        // 创建KafkaProducer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        try {
            // 创建ProducerRecord并发送消息
            ProducerRecord<String, String> record = new ProducerRecord<>(produceMockMessageDto.getTopicName(), produceMockMessageDto.getMessage());
            producer.send(record); // 异步发送消息

            // 确认消息是否发送成功
            return "Topic: "+produceMockMessageDto.getTopicName()+"，消息发送成功";
        } catch (Exception e) {
            e.printStackTrace();
            return "Topic: "+produceMockMessageDto.getTopicName()+"，消息发送失败";
        } finally {
            producer.close(); // 关闭Producer，释放资源
        }
    }


}
