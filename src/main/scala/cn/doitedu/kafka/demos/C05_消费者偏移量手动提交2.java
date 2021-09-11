package cn.doitedu.kafka.demos;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class C05_消费者偏移量手动提交2 {
    public static void main(String[] args) {


        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"doit01:9092,doit02:9092,doit03:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 如果需要手动提交消费偏移量，则需要将本参数置为false（禁用自动提交）
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");


        // 如果没有找到之前记录的偏移量，就从这个参数指定的位置开始消费
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        // 消费者的唯一标识
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG,"c01");
        // 消费者所属的组id
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"g3");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Arrays.asList("tpc3"));


        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            if(!records.isEmpty()){
                for (ConsumerRecord<String, String> record : records) {

                    // 数据处理逻辑
                    System.out.println(record.partition() + " -> " + record.offset() + " -> " + record.value());

                    // 如果在处理成功后，手动更新偏移量，则能实现  at least once 的数据处理语义
                    // 提交方式：同步调用
                    Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
                    consumer.commitSync(offsets);

                }

                // 也可以在这里记录新的消费偏移量

            }
        }
    }
}
