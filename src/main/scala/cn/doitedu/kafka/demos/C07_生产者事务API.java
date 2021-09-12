package cn.doitedu.kafka.demos;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

/**
 * @author 涛哥
 * @nick_name "deep as the sea"
 * @contact qq:657270652 wx:doit_edu
 * @site www.doitedu.cn
 * @date 2021-09-12
 * @desc 读kafka，处理后写入kafka场景的端到端事务控制
 */
public class C07_生产者事务API {

    public static void main(String[] args) {

        Properties props_producer = new Properties();
        props_producer.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "doit01:9092,doit02:9092,doit03:9092");
        props_producer.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props_producer.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props_producer.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 开启事务机制，一定要设置一个事务id
        props_producer.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "TRAN01");
        // 开启事务机制，一定要开启幂等性
        props_producer.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        // 构造一个输出结果用的生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(props_producer);


        Properties props_consumer = new Properties();
        props_consumer.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "doit01:9092,doit02:9092,doit03:9092");
        props_consumer.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "ga");
        props_consumer.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "c1");
        props_consumer.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props_consumer.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props_consumer.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props_consumer.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());
        // 构造一个读取数据源的消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props_consumer);

        // 订阅一个主题
        consumer.subscribe(Arrays.asList("tpc5"));

        // 初始化事务
        producer.initTransactions();

        // 准备干活
        while (true) {
            try {
                // 开始事务
                producer.beginTransaction();
                // 拉取数据
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if(records.isEmpty()) break;

                // 获取本次拉取的数据，都有哪些分区的
                Set<TopicPartition> partitions = records.partitions();

                // 构建一个hashmap用来记录每个分区所处理到的数据偏移量
                HashMap<TopicPartition, OffsetAndMetadata> offsetMap = new HashMap<>();


                for (TopicPartition partition : partitions) {
                    // 从records中获取当前遍历到的分区的所有数据
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    // 处理这一批该分区的数据
                    for (ConsumerRecord<String, String> r : partitionRecords) {
                        // 处理逻辑  -- 模拟：把数据变大写
                        String resValue = r.value().toUpperCase();
                        // 将处理结果封装成生产者消息，并写出
                        ProducerRecord<String, String> resRecord = new ProducerRecord<>("tpc6", r.key(), resValue);
                        producer.send(resRecord);

                    }

                    // 自己记录一下该分区处理到了哪个偏移量
                    offsetMap.put(partition, new OffsetAndMetadata(partitionRecords.get(partitionRecords.size() - 1).offset() + 1));
                }


                // 发送消费组偏移量信息给 “事务管理器”
                producer.sendOffsetsToTransaction(offsetMap, "ga");

                // 提交事务
                producer.commitTransaction();
            } catch (Exception e) {
                producer.abortTransaction();
                e.printStackTrace();
            }

        }
    }
}
