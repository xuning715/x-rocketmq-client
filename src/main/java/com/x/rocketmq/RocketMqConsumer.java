package com.x.rocketmq;

import com.alibaba.fastjson.JSON;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 集群（push）消费者
 * 1.同一个consumerGroup下的所有consumer平均消费topic的消息
 * 2.消息消费失败后，会进行重发
 * 3.相同topic的不同consumerGroup会组成伪广播模式
 * 4.第一次启动，从保存的消息队列头部开始消费数据
 * 非第一次启动，从该消费者上次消费的记录点开始消费
 */
public class RocketMqConsumer extends DefaultMQPushConsumer {
    private static final Logger logger = LogManager.getLogger(RocketMqConsumer.class);
    public final static String CONSUME_EXCEPTION ="RocketMq consume message excepton : ";
    private Map<String, String> consumerTopicAndTags = new HashMap<String, String>();//    private String topAndTagsString;
    private Object service;
    private String methodName;
    private String paramType;
    private Method method;
    private Class paramClass;

    public void setConsumerTopicAndTags(String consumerTopicAndTags) {
        String[] consumerTopicAndTagsArray = consumerTopicAndTags.split(",");
        for (String topicAndTags : consumerTopicAndTagsArray) {
            String[] topicTag = topicAndTags.split(":");
            if (topicTag.length > 1) {
                this.consumerTopicAndTags.put(topicTag[0], topicTag[1]);
            }
        }
//        this.topAndTagsString = topAndTagsString;
    }

    public void setService(Object service) {
        this.service = service;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public void setParamType(String paramType) {
        this.paramType = paramType;
    }

    public void init() throws Exception {
        // 参数信息
        logger.info("DefaultMQPushConsumer initialize!");
        logger.info(this.getConsumerGroup());
        logger.info(this.getNamesrvAddr());

        // 一个应用创建一个Consumer，由应用来维护此对象，可以设置为全局对象或者单例<br>
        // 注意：ConsumerGroupName需要由应用来保证唯一
        // 订阅指定MyTopic下tags等于MyTag
        // defaultMQPushConsumer.subscribe(topicName, tagName);
//        defaultMQPushConsumer.setSubscription(consumerTopicAndTags);
        this.setSubscription(consumerTopicAndTags);

        // 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br>
        // 如果非第一次启动，那么按照上次消费的位置继续消费
        // defaultMQPushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        this.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        // 设置为集群消费(区别于广播消费)
        this.setMessageModel(MessageModel.CLUSTERING);

        this.registerMessageListener(new MessageListenerConcurrently() {
            // 默认msgs里只有一条消息，可以通过设置consumeMessageBatchMaxSize参数来批量接收消息
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                MessageExt msg = msgs.get(0);
                try {
                    String body = new String(msg.getBody(), RocketMqProducer.UTF8);
                    execute(body);
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                } catch (Exception e) {
                    logger.error(CONSUME_EXCEPTION + msg.toString(), e);
//                    throw new RuntimeException(RocketMqConstant.CONSUME_EXCEPTION, e);
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
        });

        // Consumer对象在使用之前必须要调用start初始化，初始化一次即可<br>
        this.start();

        logger.info("DefaultMQPushConsumer start success!");
    }

    private void execute(String json) throws Exception {
        if (method == null) {
            paramClass = Class.forName(paramType);
            method = service.getClass().getMethod(methodName, paramClass);
        }
        Object param = JSON.parseObject(json, paramClass);
        method.invoke(service, param);
    }

}
