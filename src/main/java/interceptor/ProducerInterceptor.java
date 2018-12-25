package interceptor;

import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ProducerInterceptor {
    public static void main(String[] args) {
        //1、构建配置
        Properties props = new Properties();
        // Kafka服务端的主机名和端口号
        props.put("bootstrap.servers", "47.100.187.237:9092");
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

        //2、设置拦截链
        List<String> interceptors = new ArrayList<String>();
        interceptors.add("interceptor.TimeInterceptor");//优先过timeInteceptor
        interceptors.add("interceptor.CounterInterceptor");//再过counterInteceptor
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

        //3、消息发送者
        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);

        //4、发送消息
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("topic", Integer.toString(i), "hello " + i), new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.println("偏移量：" + recordMetadata.offset() + ": 分区" + recordMetadata.partition());
                }
            });
        }

        //5、关闭producer
        producer.close();
    }
}