package com.lcc.kafkaUI.service;

import com.lcc.kafkaUI.vo.dashborad.BrokerInfo;
import com.lcc.kafkaUI.vo.dashborad.ClusterInfo;
import com.lcc.kafkaUI.vo.dashborad.TopicInfo;

import java.util.List;
import java.util.Map;

public interface DashboardService {
    ClusterInfo getClusterInfo();

    List<BrokerInfo> getBrokerInfo();

    Map getMessageInfoWithWeek();

    Map getMessageInfoWithMonth();

    List<TopicInfo> getTopicInfo();

}
