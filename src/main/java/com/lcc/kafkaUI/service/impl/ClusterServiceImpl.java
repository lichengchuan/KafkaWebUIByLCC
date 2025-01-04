package com.lcc.kafkaUI.service.impl;

import com.lcc.kafkaUI.entity.ClusterNode;
import com.lcc.kafkaUI.entity.Topic;
import com.lcc.kafkaUI.service.ClusterService;
import com.lcc.kafkaUI.service.TopicService;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

@Service
public class ClusterServiceImpl implements ClusterService {

    @Autowired
    private AdminClient adminClient;


    @Override
    public List<ClusterNode> getNodeList() {
        List<ClusterNode> result = new ArrayList<>();
        try {
            DescribeClusterResult clusterInfo = adminClient.describeCluster();
            //获取集群中的节点
            Collection<Node> nodes = clusterInfo.nodes().get();
            for (Node node : nodes) {
                System.out.println("节点："+node.host()+":"+node.port());
                ClusterNode clusterNode = new ClusterNode();
                clusterNode.setId(node.idString());
                clusterNode.setHost(node.host());
                clusterNode.setPort(node.port());
                clusterNode.setRack(node.rack());
                result.add(clusterNode);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return result;
    }

    private ClusterNode nodeToClusterNode(Node node){
        ClusterNode clusterNode = new ClusterNode();

        clusterNode.setId(node.idString());
        clusterNode.setHost(node.host());
        clusterNode.setPort(node.port());
        clusterNode.setRack(node.rack());

        return clusterNode;
    }
}
