package com.study.distribute.kafka.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * <p>版权所有： 版权所有(C)2011-2099</p>
 * <p>公   司： 口袋购物 </p>
 * <p>内容摘要： 自定义kafka生产者</p>
 * <p>其他说明： </p>
 * <p>完成日期： 2018/1/6 18:34</p>
 *
 * @author wangqiming
 * @version v1.0
 */
public class MyProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KafkaConstants.BROKER_LIST);
        properties.put("acks", "1");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 100000; ++i) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            String msg = "我是" + i;
            System.out.println(String.format("send kafka key=%s, value=%s", i, msg));
            producer.send(new ProducerRecord<String, String>(KafkaConstants.MY_TOPIC, String.valueOf(i), msg));
        }
        producer.close();
    }
}
