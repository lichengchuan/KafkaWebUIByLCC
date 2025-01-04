package com.lcc.kafkaUI.service;

import com.lcc.kafkaUI.dto.message.ProduceMockMessageDto;
import org.springframework.web.socket.WebSocketSession;

public interface MessageService {
    String sendMessage(ProduceMockMessageDto produceMockMessageDto);

}
