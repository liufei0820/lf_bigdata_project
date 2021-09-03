package com.lf.bigdata.kafka.interceptor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @Classname MyInterceptorProducer
 * @Date 2021/9/3 2:48 下午
 * @Created by LiuFei
 */
public class MyInterceptorProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties props = new Properties();
        // 设置参数
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "a.lf.bigdata:6667,c.lf.bigdata:6667,d.lf.bigdata:6667");

        props.put("acks", "all");

        // 设置重试次数
        props.put("retries", 1);

        // 设置批次大小
        props.put("batch.size", 16384);

        // 设置等待时间
        props.put("linger.ms", 2);

        // RecordAccumulator 缓冲区小大
        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        // 拦截器
        List<String> interceptorList = new ArrayList<>();
        interceptorList.add("com.lf.bigdata.kafka.interceptor.TimeInterceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptorList);

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("test", "key" + i, "value" + i)).get();
        }

        producer.close();
    }

}
