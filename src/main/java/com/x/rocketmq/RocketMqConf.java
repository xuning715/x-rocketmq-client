package com.x.rocketmq;

import java.util.HashMap;
import java.util.Map;

public class RocketMqConf {
    public final static String UTF8 ="UTF-8";
    public final static String PRODUCE_EXCEPTION ="RocketMq produce message excepton : ";
    public final static String CONSUME_EXCEPTION ="RocketMq consume message excepton : ";

    private String namesrvAddr;
    private String producerGroup;
    private String consumerGroup;
    private String producerInstanceName;
    private String consumerInstanceName;

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getProducerInstanceName() {
        return producerInstanceName;
    }

    public void setProducerInstanceName(String producerInstanceName) {
        this.producerInstanceName = producerInstanceName;
    }

    public String getConsumerInstanceName() {
        return consumerInstanceName;
    }

    public void setConsumerInstanceName(String consumerInstanceName) {
        this.consumerInstanceName = consumerInstanceName;
    }
}
