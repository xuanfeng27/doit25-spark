package cn.doitedu.mykafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


/**
 * @ClassName: Demo01_kafka_consumer2
 * @Author: zll
 * @CreateTime: 2021/9/8 22:07
 * @Desc: java 程序
 * @Version: 1.0
 */
public class Demo01_kafka_consumer2 {
    public static void main(String[] args) {
        method();
    }

    public static void method(){
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"doit:9092,doit02:9092,doit03:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put("auto.offset.reset","latest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"test2");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("mytopic02"));

        while (true){
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : poll) {
                System.out.println(
                        record.key() + "--" +
                                record.value()+"--"+
                                record.partition()  + "--" +
                                record.offset()

                );
            }
        }
    }
}
