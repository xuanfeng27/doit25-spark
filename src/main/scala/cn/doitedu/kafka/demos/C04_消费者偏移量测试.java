package cn.doitedu.kafka.demos;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class C04_消费者偏移量测试 {
    public static void main(String[] args) {


        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"doit01:9092,doit02:9092,doit03:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 如果没有找到之前记录的偏移量，就从这个参数指定的位置开始消费
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        // 消费者的唯一标识
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG,"c01");
        // 消费者所属的组id
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"g2");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        // 订阅tpc3，但是本消费者消费哪些分区是没有固定的
        // 一旦组中的消费者发生增减，本消费者所消费的分区会被系统自动重分配  （消费组的负载重分配）
        //consumer.subscribe(Arrays.asList("tpc3"));

        // 由消费者固定住要消费的topic及分区
        TopicPartition tpc3_p0 = new TopicPartition("tpc3", 0);
        //TopicPartition tpc3_p1 = new TopicPartition("tpc3", 1);
        consumer.assign(Arrays.asList(tpc3_p0/*,tpc3_p1*/));

        // seek定位到指定的偏移量开始消费（无视之前所记录的消费偏移量）
        consumer.seek(tpc3_p0,3);

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            if(!records.isEmpty()){
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.partition() + " -> " + record.offset() + " -> " + record.value());
                }

            }
        }
    }
}
