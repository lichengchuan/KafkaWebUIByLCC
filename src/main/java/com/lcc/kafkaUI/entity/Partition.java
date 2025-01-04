package com.lcc.kafkaUI.entity;

import lombok.Data;

import java.util.List;

@Data
public class Partition {
    //分区编号
    private Integer partitionNum;
    //分区信息
    private ClusterNode leader;
    private List<ClusterNode> replicas;
    private List<ClusterNode> isr;
}
