package com.x.rocketmq;

import com.alibaba.fastjson.JSON;
import com.x.rocketmq.properties.ProducerProperties;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;

public class RocketMqProducer {
    private static final Logger logger = LogManager.getLogger(RocketMqProducer.class);
    private ProducerProperties producerProperties;

    private DefaultMQProducer defaultMQProducer;

    public RocketMqProducer() {

    }

    private void init() throws Exception {
        defaultMQProducer = new DefaultMQProducer(producerProperties.getProducerGroup());
        defaultMQProducer.setNamesrvAddr(producerProperties.getNamesrvAddr());
        defaultMQProducer.setInstanceName(producerProperties.getInstanceName());
        defaultMQProducer.start();
    }

    public void setProducerProperties(ProducerProperties producerProperties) throws Exception {
        this.producerProperties = producerProperties;
        init();
    }

    /**
     * @param topic
     * @param tags
     * @param body
     * @return
     */
    public boolean produceMessage(String topic, String tags, String body) throws Exception {
        Message msg = new Message(topic, tags, body.getBytes(RocketMqConstant.UTF8));
        SendResult sendResult = defaultMQProducer.send(msg);
        if (SendStatus.SEND_OK.equals(sendResult.getSendStatus())) {
            return true;
        } else {
            logger.error(RocketMqConstant.PRODUCE_EXCEPTION + sendResult.toString());
            return false;
        }
    }

    public boolean produceMessage(String topic, String tags, Object body) throws Exception {
        String json = JSON.toJSONString(body);
        return produceMessage(topic, tags, body);
    }

    public void destroy() {
        defaultMQProducer.shutdown();
    }

}
