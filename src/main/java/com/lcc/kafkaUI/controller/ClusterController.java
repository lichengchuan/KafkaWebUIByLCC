package com.lcc.kafkaUI.controller;

import com.lcc.kafkaUI.entity.ClusterNode;
import com.lcc.kafkaUI.service.ClusterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class ClusterController {
    @Autowired
    private ClusterService clusterService;

    @GetMapping("/findNodeList")
    public List<ClusterNode> findNodeList(){
        return clusterService.getNodeList();
    }

    @GetMapping("/findNodeListSize")
    public Integer findNodeListSize(){
        return clusterService.getNodeList().size();
    }
}
