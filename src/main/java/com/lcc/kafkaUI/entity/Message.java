package com.lcc.kafkaUI.entity;

import lombok.Data;

import java.util.Date;

@Data
public class Message {
    /**
     * 消息所属的 Topic 名称。
     */
    private String name;

    /**
     * 消息所在的分区编号。
     */
    private Integer partition;

    /**
     * 消息在分区中的偏移量，用于唯一标识该分区内的消息位置。
     */
    private Long offset;

    /**
     * 消息的时间戳，可以由生产者或 Broker 设置。
     */
    private Long timestamp;

    /**
     * 基于时间戳生成Date对象
     */
    private Date date;

    /**
     * 时间戳的类型，指示时间戳是由生产者设置（CREATE_TIME）还是由 Broker 设置（LOG_APPEND_TIME）。
     */
    private String timestampType;

    /**
     * 消息键序列化后的大小（字节数）。
     * 如果键为空，则该值为 -1。
     */
    private Integer serializedKeySize;

    /**
     * 消息值序列化后的大小（字节数）。
     * 如果值为空，则该值为 -1。
     */
    private Integer serializedValueSize;

    /**
     * 消息头部信息，包含以键值对形式存储的元数据。
     */
    private String headers;

    /**
     * 消息的键（可选）。
     * 用于标识消息，通常在分区策略中使用。
     */
    private String key;

    /**
     * 消息的值，表示消息的实际内容。
     */
    private String value;
}
