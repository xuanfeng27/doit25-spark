package cn.doitedu.mykafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @ClassName: Demo01_kafka_producer
 * @Author: zll
 * @CreateTime: 2021/9/8 20:24
 * @Desc: java 程序
 * @Version: 1.0
 */
public class Demo01_kafka_producer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"doit:9092,doit02:9092,doit03:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        for(int i=0;i<=100;i++){
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    "mytopic02",
                    "key"+i,
                    "value"+i
            );
            producer.send(record);
        }



        producer.close();
    }
}
