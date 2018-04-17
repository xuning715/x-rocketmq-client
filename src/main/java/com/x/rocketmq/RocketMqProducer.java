package com.x.rocketmq;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;

public class RocketMqProducer extends DefaultMQProducer {
    private static final Logger logger = LogManager.getLogger(RocketMqProducer.class);
    public final static String UTF8 ="UTF-8";
    public final static String PRODUCE_EXCEPTION ="RocketMq produce message excepton : ";

    public RocketMqProducer() {

    }

    private void init() throws Exception {
        this.start();
    }

    /**
     * @param topic
     * @param tags
     * @param body
     * @return
     */
    public boolean produceMessage(String topic, String tags, String body) throws Exception {
        Message msg;
        if (tags == null || tags.length() == 0) {
            msg = new Message(topic, body.getBytes(UTF8));
        } else {
            msg = new Message(topic, tags, body.getBytes(UTF8));
        }
        SendResult sendResult = this.send(msg);
        if (SendStatus.SEND_OK.equals(sendResult.getSendStatus())) {
            return Boolean.TRUE;
        } else {
            logger.error(PRODUCE_EXCEPTION + sendResult.toString());
            return Boolean.FALSE;
        }
    }

    public boolean produceMessage(String topic, String tags, Object body) throws Exception {
        String json = JSON.toJSONString(body);
        return produceMessage(topic, tags, json);
    }

}
