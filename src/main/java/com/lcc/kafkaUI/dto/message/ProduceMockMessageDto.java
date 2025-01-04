package com.lcc.kafkaUI.dto.message;

import lombok.Data;

@Data
public class ProduceMockMessageDto {
    private String topicName;
    private String message;//消息内容
}
