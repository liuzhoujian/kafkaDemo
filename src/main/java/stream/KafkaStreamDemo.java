package stream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

//发送端（linux命令行建立生产者） ---》处理（主要通过TopologyBuilder,建立处理链） ---》消费端（linux命令行建立消费者）

public class KafkaStreamDemo {
    public static void main(String[] args) {
        //1、配置信息
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "47.100.187.237:9092,47.100.187.237:9093");

        StreamsConfig config = new StreamsConfig(props);

        //2、构建拓扑
        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("SOURCE", "source-topic") //从主题为first中读取数据
                .addProcessor("PROCESSOR", new ProcessorSupplier<byte[], byte[]>() {
                    public Processor<byte[], byte[]> get() {
                        return new LogProcessor();//具体处理逻辑
                    }
                }, "SOURCE")
                .addSink("SINK", "sink-topic", "PROCESSOR"); //将处理后的数据存入到second主题中

        //3、创建kafka stream
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();//开启stream
    }
}
