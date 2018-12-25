package producer.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class MyPartitionImpl implements Partitioner {
    //初始化
    public void configure(Map<String, ?> map) {
    }

    //业务逻辑 返回值指的是分区编号
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        //根据某些条件，判断哪些数据存放在不同的分区
      /*  if(){
            return 0; 存到0分区
        } else {
            return 1; 存到1分区
        }*/


        return 1;//指定存在1分区
    }

    //关闭资源
    public void close() {
    }
}
