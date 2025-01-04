package com.lcc.kafkaUI.entity;

import lombok.Data;

@Data
public class Topic {
    private String name;
    private Boolean isInternal;
}
