package stream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * 日志处理
 */
public class LogProcessor implements Processor<byte[], byte[]> {
    private ProcessorContext processorContext;

    public void init(ProcessorContext processorContext) {
        this.processorContext = processorContext;
    }


    public void process(byte[] key, byte[] value) {
        String input = new String(value);
        System.out.println("输入数据：" + input);

        if(input.contains(">>>")) { //如果有>>> 则进行数据清洗
            input = input.split(">>>")[1].trim();
            processorContext.forward(key, input.getBytes()); //输出到下一个主题
        } else {
            processorContext.forward(key, value);
        }
    }

    public void punctuate(long l) {

    }

    public void close() {

    }
}
