package com.lf.bigdata.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @Classname MyAutoCommitConsumer
 * @Date 2021/9/3 2:03 下午
 * @Created by LiuFei
 */
public class MyAutoCommitConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "a.lf.bigdata:6667,b.lf.bigdata:6667,c.lf.bigdata:6667");

        // 指定group id
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test_group");
        // 指定初次消费从哪里
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 是否开启自动commit
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        // 创建consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        // 订阅Topic
        consumer.subscribe(Arrays.asList("test"));
        // 循环获取消息
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n",record.offset(),record.key(),record.value());
            }
        }
    }
}
