package com.lcc.kafkaUI.vo.dashborad;

import lombok.Data;

/**
 * 大屏看板：集群概况
 */
@Data
public class ClusterInfo {
    //集群id
    private String id;
    //controller节点
    private String controller;
    //broker总数
    private Integer brokerCount;
    //Topic总数
    private Integer topicCount;
}
