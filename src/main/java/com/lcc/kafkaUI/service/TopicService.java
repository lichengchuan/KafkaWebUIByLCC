package com.lcc.kafkaUI.service;

import com.lcc.kafkaUI.dto.topic.TopicCreateDto;
import com.lcc.kafkaUI.entity.Partition;
import com.lcc.kafkaUI.entity.Topic;

import java.util.List;
import java.util.Map;

public interface TopicService {
    /**
     * 获取Topic列表
     * @param isInternal 是否查询内部Topic
     * @return
     */
    List<Topic> getTopicList(boolean isInternal);

    /**
     * 根据topicName查询指定Topic的消息列表
     * @return
     */
    Map<String,Object> getMessageByTopic(String topicName, String startDate, String endDate, int pageNumber, int pageSize);

    /**
     * 创建Topic
     */
    String createTopic(TopicCreateDto topicCreateDto);

    String deleteTopics(List<String> topicNameList);

    List<Partition> getTopicInfo(String topicName);

    String increasePartitions(Integer num,String topicName);

    boolean isExistTopic(String topicName);
}
