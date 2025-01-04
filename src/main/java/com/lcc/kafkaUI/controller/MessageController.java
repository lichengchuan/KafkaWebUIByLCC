package com.lcc.kafkaUI.controller;

import com.lcc.kafkaUI.dto.message.ProduceMockMessageDto;
import com.lcc.kafkaUI.service.MessageService;
import com.lcc.kafkaUI.service.TopicService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
public class MessageController {

    @Autowired
    private TopicService topicService;

    @Autowired
    private MessageService messageService;

    @GetMapping("/findMessageByTopic")
    public Map<String, Object> findMessageByTopic(@RequestParam(value = "topicName",required = true) String topicName,
                                                  @RequestParam(value = "pageNum",required = true) Integer pageNum,
                                                  @RequestParam(value = "pageSize",required = true) Integer pageSize,
                                                  @RequestParam(value = "startDate",required = false) String startDate,
                                                  @RequestParam(value = "endDate",required = false) String endDate){
        return topicService.getMessageByTopic(topicName,startDate,endDate,pageNum,pageSize);
    }

    @PostMapping("/producerMockSendMessage")
    public String produceMockSendMessage(@RequestBody ProduceMockMessageDto produceMockMessageDto){
        if (produceMockMessageDto.getTopicName() == null || produceMockMessageDto.getTopicName().equals("")){
            return "非法topic name";
        }
        if (!topicService.isExistTopic(produceMockMessageDto.getTopicName())){
            return "该Topic不存在";
        }
        if (produceMockMessageDto.getMessage() == null || produceMockMessageDto.getMessage().equals("")){
            return "发送消息不能为空";
        }
        return messageService.sendMessage(produceMockMessageDto);
    }

}
