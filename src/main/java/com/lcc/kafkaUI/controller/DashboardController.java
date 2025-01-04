package com.lcc.kafkaUI.controller;

import com.lcc.kafkaUI.service.DashboardService;
import com.lcc.kafkaUI.vo.dashborad.BrokerInfo;
import com.lcc.kafkaUI.vo.dashborad.ClusterInfo;
import com.lcc.kafkaUI.vo.dashborad.TopicInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/dashboard")
public class DashboardController {

    @Autowired
    private DashboardService dashboardService;

    @GetMapping("/clusterInfo")
    public ClusterInfo getClusterInfo(){
        return dashboardService.getClusterInfo();
    }

    @GetMapping("/brokerInfo")
    public List<BrokerInfo> getBrokerInfo(){
        return dashboardService.getBrokerInfo();
    }

    @GetMapping("/messageInfo")
    public Map getBrokerInfo(@RequestParam("way") String way){
        if ("week".equals(way)){
            return dashboardService.getMessageInfoWithWeek();
        }else{
            return dashboardService.getMessageInfoWithMonth();
        }
    }

    @GetMapping("/topicInfo")
    public List<TopicInfo> getTopicInfo(){
        return dashboardService.getTopicInfo();
    }

}
