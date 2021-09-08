package cn.doitedu.kafka.demos;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class C02_KAFKA消费者示例 {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"doit01:9092,doit02:9092,doit03:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"g01");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        // 一次拉取，最大数据量
        props.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,"41943040");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        // 订阅一个或多个主题
        consumer.subscribe(Arrays.asList("doitedu-tpc2"));

        while(true) {
            // 拉取（消费）数据  （一次poll会拉取到一批record）
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.key() + " , " + record.value() + " , " + record.partition() + " , " + record.offset());
            }


        }

    }
}
