package com.lcc.kafkaUI.controller;

import com.lcc.kafkaUI.dto.topic.TopicCreateDto;
import com.lcc.kafkaUI.entity.Partition;
import com.lcc.kafkaUI.entity.Topic;
import com.lcc.kafkaUI.service.TopicService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.List;

@RestController
public class TopicController {

    @Autowired
    private TopicService topicService;

    @GetMapping("/findTopicList")
    public List<Topic> findTopicList(@RequestParam(value = "isInternal",required = false) Boolean isInternal){
        return topicService.getTopicList(isInternal==null?true:isInternal);
    }

    @GetMapping("/findTopicInfo")
    public List<Partition> findTopicInfo(@RequestParam(value = "topicName",required = true) String topicName){
        if (topicName == null || topicName.equals("")){
            return null;
        }
        return topicService.getTopicInfo(topicName);
    }

    @GetMapping("/increasePartitions/{num}")
    public String increasePartitions(@PathVariable Integer num,@RequestParam(value = "topicName",required = true) String topicName){
        if (topicName == null || topicName.equals("")){
            return "非法topic name";
        }
        return topicService.increasePartitions(num,topicName);
    }

    @PostMapping("/createTopic")
    public String createTopic(@RequestBody TopicCreateDto topicCreateDto){
        if (topicCreateDto.getTopicName() == null || topicCreateDto.getTopicName().equals("")){
            return "非法topic name";
        }
        if (topicCreateDto.getPartitions() == null || topicCreateDto.getPartitions()<=0){
            topicCreateDto.setPartitions(1);
        }
        if (topicCreateDto.getReplication() == null || topicCreateDto.getReplication()<=0){
            topicCreateDto.setReplication((short) 1);
        }
        return topicService.createTopic(topicCreateDto);
    }

    @DeleteMapping("/deleteTopics")
    public String deleteTopics(@RequestParam(value = "topicNameList",required = true) String topicNameList){
        if (topicNameList == null || topicNameList.equals("")){
            return "非法topic name";
        }
        String[] split = topicNameList.split(",");
        return topicService.deleteTopics(Arrays.asList(split));
    }

}