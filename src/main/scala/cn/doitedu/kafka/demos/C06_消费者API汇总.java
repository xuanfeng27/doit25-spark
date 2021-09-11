package cn.doitedu.kafka.demos;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

public class C06_消费者API汇总 {

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
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG,"c02");
        // 消费者所属的组id
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"g3");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);


        // 获取本消费者之前记录的消费偏移量
        System.out.println("================== g3 消费组录的在 tpc3 partition0 上的消费偏移量 =============");
        TopicPartition tpc3_p0 = new TopicPartition("tpc3", 0);
        OffsetAndMetadata offset = consumer.committed(tpc3_p0);
        System.out.println(offset);


        /**
         *
         * 以下是一些 集群元信息查询api
         *
         * 主要用于开发定制的kafka管理平台
         *
         */

        // 查询服务端指定partition上的最大偏移量（结束偏移量）
        System.out.println("================== tpc3 partition0 的最大偏移量 =============");
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(Arrays.asList(tpc3_p0));
        System.out.println(endOffsets.get(tpc3_p0));


        // 查询服务端指定partition上的最小偏移量（起始偏移量）
        System.out.println("================== tpc3 partition0 的最小偏移量 =============");
        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(Arrays.asList(tpc3_p0));
        System.out.println(beginningOffsets.get(tpc3_p0));


        // 查询tpc3的分区信息
        List<PartitionInfo> tpc3 = consumer.partitionsFor("tpc3");


        // 查询本消费者所被分配消费的分区集合
        Set<TopicPartition> assignment = consumer.assignment();


        // 查询集群中都有哪些topic
        Map<String, List<PartitionInfo>> topics = consumer.listTopics();


        // 根据消息的时间戳来查询对应的偏移量
        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumer.offsetsForTimes(Collections.singletonMap(tpc3_p0, 16713273658237L));


        consumer.close();



    }

}
