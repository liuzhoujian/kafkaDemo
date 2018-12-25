package interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class CounterInterceptor implements ProducerInterceptor {

    private int successCnt = 0;
    private int failureCnt = 0;

    public void configure(Map<String, ?> map) {

    }

    public ProducerRecord onSend(ProducerRecord producerRecord) {
        return producerRecord; //将消息原样放行
    }

    //统计成功或者失败的次数
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if(e == null) {
            successCnt++;
        } else {
            failureCnt++;
        }
    }

    public void close() {
        System.out.println("成功的次数：" + successCnt + "，失败的次数：" + failureCnt);
    }
}
