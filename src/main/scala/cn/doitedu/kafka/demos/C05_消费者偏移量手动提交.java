package cn.doitedu.kafka.demos;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class C05_消费者偏移量手动提交 {
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
        // 分区分配策略： 区间策略
        props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,RoundRobinAssignor.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);




        Properties props2 = new Properties();
        props2.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,RangeAssignor.class.getName());
        props2.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"g3");
        KafkaConsumer<String, String> consumer2 = new KafkaConsumer<String, String>(props2);

        // 由消费者固定住要消费的topic及分区
        TopicPartition tpc3_p0 = new TopicPartition("tpc3", 0);
        //TopicPartition tpc3_p1 = new TopicPartition("tpc3", 1);
        consumer.assign(Arrays.asList(tpc3_p0/*,tpc3_p1*/));


        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            if(!records.isEmpty()){
                for (ConsumerRecord<String, String> record : records) {
                    // 如果在处理成功前，手动更新偏移量，则能实现  at most once 的数据处理语义


                    // 数据处理逻辑
                    System.out.println(record.partition() + " -> " + record.offset() + " -> " + record.value());

                    // 如果在处理成功后，手动更新偏移量，则能实现  at least once 的数据处理语义
                    // 提交方式：同步调用
                    Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(tpc3_p0, new OffsetAndMetadata(record.offset() + 1));
                    //consumer.commitSync(offsets);

                    // 提交方式：异步调用
                    consumer.commitAsync(offsets, new OffsetCommitCallback() {
                        @Override
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                            System.out.println("偏移量已经提交成功.....");
                        }
                    });


                }

            }
        }
    }
}
