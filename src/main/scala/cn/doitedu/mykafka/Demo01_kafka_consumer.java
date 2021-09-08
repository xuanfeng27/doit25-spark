package cn.doitedu.mykafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * @ClassName: Demo01_kafka
 * @Author: zll
 * @CreateTime: 2021/9/8 16:59
 * @Desc: Kafka是一个分布式消息中间件,支持分区的、多副本的、多订阅者的、基于zookeeper协调的分布式消息系统。
 * kafka是发布-订阅模式。kafka只能保证一个partition内的消息有序性
 * @Version: 1.0
 */
public class Demo01_kafka_consumer {
    public static void main(String[] args) {
        ThreadPoolExecutor pools = new ThreadPoolExecutor(
                3,
                4,
                1L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(3)
        );
        pools.submit(new Runnable() {
            @Override
            public void run() {
                method();
            }
        });

        pools.submit(new Runnable() {
            @Override
            public void run() {
                method();
            }
        });


    }

    public static void method(){
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"doit:9092,doit02:9092,doit03:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put("auto.offset.reset","latest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"test");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("mytopic02"));

        while (true){
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : poll) {
                System.out.println(
                        record.key() + "--" +
                                record.value()+"--"+
                                record.partition()  + "--" +
                                record.offset() + "--" +
                                Thread.currentThread().getName()

                );
            }
        }
    }
}
