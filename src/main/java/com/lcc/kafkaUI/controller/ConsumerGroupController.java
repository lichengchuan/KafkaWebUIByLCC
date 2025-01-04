package com.lcc.kafkaUI.controller;

import com.lcc.kafkaUI.dto.ConsumerGroup.ConsumerGroupOffsetDto;
import com.lcc.kafkaUI.entity.ConsumerGroup;
import com.lcc.kafkaUI.entity.ConsumerGroupTopicPartition;
import com.lcc.kafkaUI.service.ConsumerGroupService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.List;

@RestController
public class ConsumerGroupController {

    @Autowired
    private ConsumerGroupService consumerGroupService;

    @GetMapping("/findConsumerGroupList")
    public List<ConsumerGroup> findConsumerGroupList(){
        return consumerGroupService.getConsumerGroupList();
    }

    @GetMapping("/findConsumerGroupOffset")
    public List<ConsumerGroupTopicPartition> findConsumerGroupOffset(@RequestParam(value = "groupId",required = true) String groupId){
        return consumerGroupService.getConsumerGroupOffset(groupId);
    }

    @DeleteMapping("/deleteConsumerGroups")
    public String deleteConsumerGroups(@RequestParam(value = "consumerGroupIdList",required = true) String consumerGroupIdList){
        if (consumerGroupIdList == null || consumerGroupIdList.equals("")){
            return "操作失败";
        }
        String[] split = consumerGroupIdList.split(",");
        return consumerGroupService.deleteConsumerGroups(Arrays.asList(split));
    }

    @PostMapping("/updateCommittedOffset")
    public String updateCommittedOffset(@RequestBody ConsumerGroupOffsetDto consumerGroupOffsetDto){
        if (StringUtils.isEmpty(consumerGroupOffsetDto.getConsumerGroupId()) || StringUtils.isEmpty(consumerGroupOffsetDto.getTopic())){
            return "操作失败";
        }
        if (consumerGroupOffsetDto.getOffset() < 0){
            return "操作失败";
        }
        if (consumerGroupOffsetDto.getPartitionNum() < 0){
            return "操作失败";
        }
        return consumerGroupService.updateCommittedOffset(consumerGroupOffsetDto);
    }
}
