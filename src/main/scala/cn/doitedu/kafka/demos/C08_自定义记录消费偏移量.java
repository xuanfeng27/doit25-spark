package cn.doitedu.kafka.demos;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.time.Duration;
import java.util.*;

public class C08_自定义记录消费偏移量 {

    public static void main(String[] args) throws Exception {

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
        // 禁用自动提交消费偏移量
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        ZooKeeper zooKeeper = new ZooKeeper("doit01:2181,doit02:2181", 2000, null);

        // 订阅主题
        consumer.subscribe(Arrays.asList("tpc6"));


        // 先去获取之前记录的偏移量
        // { "tpc6_0":1873485,"tpc6_1":17438756,"tpc6_2":17834856 }
        byte[] offsetsBytes = zooKeeper.getData("/myoffset", null, null);
        String offsetJson = new String(offsetsBytes);
        HashMap<String, Long> offsetMapOrgin = JSON.parseObject(offsetJson, new TypeReference<HashMap<String, Long>>() {});

        // 根据读取到的偏移量，对consumer进行各分区的seek定位
        Set<Map.Entry<String, Long>> entries = offsetMapOrgin.entrySet();
        for (Map.Entry<String, Long> entry : entries) {
            String key = entry.getKey();
            String[] topicAndPartition = key.split("_");

            consumer.seek(new TopicPartition(topicAndPartition[0],Integer.parseInt(topicAndPartition[1])),entry.getValue());
        }

        while(true) {
            // 拉取数据
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            // 处理数据
            Set<TopicPartition> partitions = records.partitions();

            HashMap<String, Long> offsets = new HashMap<>();

            for (TopicPartition partition : partitions) {
                List<ConsumerRecord<String, String>> datas = records.records(partition);
                for (ConsumerRecord<String, String> data : datas) {
                    // 处理

                }
                ConsumerRecord<String, String> lastRecord = datas.get(datas.size() - 1);
                long offset = lastRecord.offset();
                offsets.put(partition.topic() + "_" + partition.partition(), offset);
            }

            // 把偏移量记入zookeeper
            offsetJson = JSON.toJSONString(offsets);

            zooKeeper.create("/myoffset", offsetJson.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

    }
}
