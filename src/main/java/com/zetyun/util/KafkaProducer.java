package com.zetyun.util;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

public class KafkaProducer {
    public static Producer<String, String> kafkaProducer;
    public static int num;

    public static void main(String[] args) throws InterruptedException {
        System.out.println("注意点：第一个参数是kafka地址，第二个参数是kafka topic，第三个参数发送的条数");
        System.out.println("kafka地址:" + args[0] + "    topic:" + args[1] + "   发送的条数:" + args[2]);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", args[0]);
        properties.put("ack", "all");
        properties.put("linger.ms", "0");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(properties);
        num = Integer.parseInt(args[2]);
        for (int i = 0; i < Integer.parseInt(args[2]); i++) {
            //延时100毫秒，不要发送太快
            Thread.sleep(100);
            String message = eventTime();
            //发送数据到kafka
            kafkaProducer.send(new ProducerRecord<>(args[1].toString(),message));
            //已发送多少条
            if (Integer.parseInt(args[2]) > 10) {
                if ((i % (Integer.parseInt(args[2]) / 10) == 0)) {
                    System.out.println("已发送" + i + "条！");
                }
            }
        }
        kafkaProducer.flush();
        kafkaProducer.close();
        System.out.println("已发送" + num + "条！");

    }


    public static String eventTime() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("{\"session_id\":\"" +
                UUID.randomUUID().toString().replace("-", "") +
                "\",\"name\":\"liu\",\"event_time\":\"" +
                getDate() +
                "\"}");
        return buffer.toString();
    }

    public static String getDate() {
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String format = sdf.format(date);
        return format;
    }

    ;
}
