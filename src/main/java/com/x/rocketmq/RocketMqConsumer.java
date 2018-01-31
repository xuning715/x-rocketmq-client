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
public class RocketMqConsumer {
    private static final Logger logger = LogManager.getLogger(RocketMqConsumer.class);
    private RocketMqConf rocketMqConf;
    private Map<String, String> consumerTopicAndTags = new HashMap<String, String>();//    private String topAndTagsString;
    private Object service;
    private String methodName;
    private String paramType;
    private Method method;
    private Class paramClass;

    private DefaultMQPushConsumer defaultMQPushConsumer;

    public void setRocketMqConf(RocketMqConf rocketMqConf) {
        this.rocketMqConf = rocketMqConf;
    }

    public void setConsumerTopicAndTags(String consumerTopicAndTags) {
        String[] consumerTopicAndTagsArray = consumerTopicAndTags.split(",");
        for (String topicAndTags : consumerTopicAndTagsArray) {
            String[] topicTag = topicAndTags.split(":");
            this.consumerTopicAndTags.put(topicTag[0], topicTag[1]);
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
        logger.info(rocketMqConf.getConsumerGroup());
        logger.info(rocketMqConf.getNamesrvAddr());

        // 一个应用创建一个Consumer，由应用来维护此对象，可以设置为全局对象或者单例<br>
        // 注意：ConsumerGroupName需要由应用来保证唯一
        defaultMQPushConsumer = new DefaultMQPushConsumer(rocketMqConf.getConsumerGroup());
        defaultMQPushConsumer.setNamesrvAddr(rocketMqConf.getNamesrvAddr());
        defaultMQPushConsumer.setInstanceName(rocketMqConf.getConsumerInstanceName());

        // 订阅指定MyTopic下tags等于MyTag

//        defaultMQPushConsumer.subscribe(topicName, tagName);
        defaultMQPushConsumer.setSubscription(consumerTopicAndTags);

        // 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br>
        // 如果非第一次启动，那么按照上次消费的位置继续消费
//        defaultMQPushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        defaultMQPushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        // 设置为集群消费(区别于广播消费)
        defaultMQPushConsumer.setMessageModel(MessageModel.CLUSTERING);

        defaultMQPushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            // 默认msgs里只有一条消息，可以通过设置consumeMessageBatchMaxSize参数来批量接收消息
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                MessageExt msg = msgs.get(0);
                try {
                    String body = new String(msg.getBody(), RocketMqConf.UTF8);
                    execute(body);
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                } catch (Exception e) {
                    logger.error(RocketMqConf.CONSUME_EXCEPTION + msg.toString(), e);
//                    throw new RuntimeException(RocketMqConstant.CONSUME_EXCEPTION, e);
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
        });

        // Consumer对象在使用之前必须要调用start初始化，初始化一次即可<br>
        defaultMQPushConsumer.start();

        logger.info("DefaultMQPushConsumer start success!");
    }

    /**
     * Spring bean destroy-method
     */
    public void destroy() {
        defaultMQPushConsumer.shutdown();
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
