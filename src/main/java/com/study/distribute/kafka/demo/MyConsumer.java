package com.study.distribute.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.*;

/**
 * <p>版权所有： 版权所有(C)2011-2099</p>
 * <p>公   司： 口袋购物 </p>
 * <p>内容摘要： 自定义kafka消费者</p>
 * <p>其他说明： </p>
 * <p>完成日期： 2018/1/6 18:44</p>
 *
 * @author wangqiming
 * @version v1.0
 */
public class MyConsumer {

    public static void main(String[] args) {
//        autoCommitClient();
        manualCommitClient();
    }

    /**
     * 手动提交
     */
    public static void manualCommitClient() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KafkaConstants.BROKER_LIST);
        properties.put("group.id", "haha3");
        // 关掉自动提交
        properties.put("enable.auto.commit", "false");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "latest");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        final int minBatchSize = 10;
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(KafkaConstants.MY_TOPIC));

        List<ConsumerRecord<String, String>> bufferList = new ArrayList<>();
        while (true) {
            System.out.println("-----start pull-----");
            long starttime = System.currentTimeMillis();
            // 如果没有接收到消息，则等待1秒钟
            ConsumerRecords<String, String> records = consumer.poll(1000);
            long endtime = System.currentTimeMillis();
            long tm = endtime - starttime;
            System.out.println("-----start pull-----");
            System.out.println("tm=" + tm / 1000);

            for (ConsumerRecord<String, String> record : records) {
                System.out.println(String.format("partition=%s, offset=%d, key=%s, value=%s",
                        record.partition(), record.offset(), record.key(), record.value()));
                bufferList.add(record);
            }
            System.out.println("buffer size=" + bufferList.size());

            // 如果读取到的消息有10条，就进行处理
            if (bufferList.size() >= minBatchSize) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                System.out.println("-----manual commit start-----");
                // 处理完之后进行提交
                consumer.commitAsync();
                bufferList.clear();
                System.out.println("-----manual commit end-----");
            }
        }
    }

    /**
     * 自动提交
     */
    public static void autoCommitClient() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KafkaConstants.BROKER_LIST);
        properties.put("group.id", "haha2");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(KafkaConstants.MY_TOPIC));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> consumerRecord : records) {
                System.out.println(String.format("offset=%d, key=%s, value=%s, partition=%d",
                        consumerRecord.offset(), consumerRecord.key(), consumerRecord.value(), consumerRecord.partition()));
            }
        }
    }
}
