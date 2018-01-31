package com.x.rocketmq;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;

public class RocketMqProducer {
    private static final Logger logger = LogManager.getLogger(RocketMqProducer.class);
    private RocketMqConf rocketMqConf;

    private DefaultMQProducer defaultMQProducer;

    public RocketMqProducer() {

    }

    public void setRocketMqConf(RocketMqConf rocketMqConf) {
        this.rocketMqConf = rocketMqConf;
    }

    private void init() throws Exception {
        defaultMQProducer = new DefaultMQProducer(this.rocketMqConf.getProducerGroup());
        defaultMQProducer.setNamesrvAddr(this.rocketMqConf.getNamesrvAddr());
        defaultMQProducer.setInstanceName(this.rocketMqConf.getProducerInstanceName());
        defaultMQProducer.start();
    }

    /**
     * @param topic
     * @param tags
     * @param body
     * @return
     */
    public boolean produceMessage(String topic, String tags, String body) throws Exception {
        Message msg;
        if (StringUtils.isEmpty(tags)) {
            msg = new Message(topic, body.getBytes(RocketMqConf.UTF8));
        } else {
            msg = new Message(topic, tags, body.getBytes(RocketMqConf.UTF8));
        }
        SendResult sendResult = defaultMQProducer.send(msg);
        if (SendStatus.SEND_OK.equals(sendResult.getSendStatus())) {
            return Boolean.TRUE;
        } else {
            logger.error(RocketMqConf.PRODUCE_EXCEPTION + sendResult.toString());
            return Boolean.FALSE;
        }
    }

    public boolean produceMessage(String topic, String tags, Object body) throws Exception {
        String json = JSON.toJSONString(body);
        return produceMessage(topic, tags, json);
    }

    public void destroy() {
        defaultMQProducer.shutdown();
    }

}
