package com.lcc.kafkaUI.service;

import com.lcc.kafkaUI.entity.ClusterNode;
import com.lcc.kafkaUI.entity.Topic;

import java.util.List;

public interface ClusterService {
    /**
     * 获取集群下的所有Broker列表
     * @return
     */
    List<ClusterNode> getNodeList();
}
