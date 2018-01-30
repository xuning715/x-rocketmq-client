package com.x.rocketmq.properties;

import java.util.HashMap;
import java.util.Map;

public class ConsumerProperties {
    private String namesrvAddr;
    private String consumerGroup;
    private String instanceName;
    //    private String topAndTagsString;
    private Map<String, String> topicAndTags = new HashMap<String, String>();
    private Object service;
    private String methodName;
    private String paramType;


    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public void setTopicAndTags(String topicAndTags) {
        String[] topicAndTagsArray = topicAndTags.split(",");
        for (String topicAndTag : topicAndTagsArray) {
            String[] topicTag = topicAndTag.split(":");
            this.topicAndTags.put(topicTag[0], topicTag[1]);
        }
//        this.topAndTagsString = topAndTagsString;

    }

    public String getNamesrvAddr() {
        return this.namesrvAddr;
    }

    public String getConsumerGroup() {
        return this.consumerGroup;
    }

    public String getInstanceName() {
        return this.instanceName;
    }

    public Map<String, String> getTopicAndTags() {
        return this.topicAndTags;
    }

    public Object getService() {
        return service;
    }

    public void setService(Object service) {
        this.service = service;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public String getParamType() {
        return paramType;
    }

    public void setParamType(String paramType) {
        this.paramType = paramType;
    }
}
