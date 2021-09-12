package cn.doitedu.kafka.demos;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class C01_KAFKA生产者示例 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties props = new Properties();
        //props.setProperty("bootstrap.servers","doit01:9092,doit02:9092,doit03:9092");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"doit01:9092,doit02:9092,doit03:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // acks参数的可选值有： 0 ， 1，  -1（all）
        // 0: 不等待服务端的确认
        // 1: 等确认，而且服务端只要leader存好这条消息，就确认； -- 可靠性高于0，但也不能确保数据一定不丢
        // 2:
        props.setProperty(ProducerConfig.ACKS_CONFIG,"1");
        props.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,"1");
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        props.setProperty(ProducerConfig.RETRIES_CONFIG,"3");


        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        boolean flag = true;
        for(int i=0;i<1000000;i++) {
            // 构造一条消息
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(
                    "doitedu-tpc2",
                    StringUtils.leftPad(i+"",8,"0"),
                    RandomStringUtils.randomAlphabetic(2).toUpperCase());

            // 用生产者发送消息(kafka的producer底层数据发送都是使用的异步发送）
            producer.send(record);

            Thread.sleep(200);

        }
        producer.close();

    }
}
