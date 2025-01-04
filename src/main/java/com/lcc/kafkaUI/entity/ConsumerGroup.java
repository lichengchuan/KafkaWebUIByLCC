package com.lcc.kafkaUI.entity;


import lombok.Data;

import java.util.List;

@Data
public class ConsumerGroup {
    //消费者组id
    private String groupId;
    //是否为“简单消费者组”，简单消费者组 是一种特殊情况，指消费者组中只有一个消费者，且该消费者没有分区分配的逻辑。
    private boolean isSimpleConsumerGroup;
    //当前消费者组中所有成员的信息
    private List<ConsumerClient> members;
    //当前消费者组使用的分区分配策略，range：按范围分配、roundrobin：轮询分配、sticky：粘性分配，尽量减少分区重新分配的变化。
    private String partitionAssignor;

    //消费者组的所处状态
    /**
     * Stable：消费者组已稳定，分区分配已完成。
     * CompletingRebalance：消费者组正在完成分区重新分配。
     * PreparingRebalance：消费者组准备进行分区重新分配。
     * Empty：消费者组中没有任何活跃成员。
     * Dead：消费者组已被删除或不再活跃。
     */
    private String state;

    //负责协调该消费者组的 Kafka Broker 节点
    private ClusterNode coordinator;

}
