package producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class CallbackKafkaProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        // Kafka服务端的主机名和端口号
        props.put("bootstrap.servers", "47.100.187.237:9092,47.100.187.237:9093");
        // 等待所有副本节点的应答
        props.put("acks", "all");
        // 消息发送最大尝试次数
        props.put("retries", 0);
        // 一批消息处理大小
        props.put("batch.size", 16384);
        // 请求延时
        props.put("linger.ms", 1);
        // 发送缓存区内存大小
        props.put("buffer.memory", 33554432);
        // key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);

        for (int i = 0; i < 50; i++) {
            producer.send(new ProducerRecord<String, String>("lzjtopic", Integer.toString(i), "hello " + i), new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.println("偏移量：" + recordMetadata.offset() + ": 分区" + recordMetadata.partition());
                }
            });
        }

        //关闭producer
        producer.close();
    }
}
/**
 * 在java代码中，主体可以不用提前在kafka中设置，java代码会自动创建
 */