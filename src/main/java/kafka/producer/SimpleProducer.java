package kafka.producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.clients.producer.internals.Partitioner;

import java.util.Properties;


/**
 * Created by root on 2018/4/1.
 *
 * kafka生产者
 */
public class SimpleProducer {

    private final KafkaProducer<Integer, String> producer;

    public SimpleProducer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "node1:9092,node2:9092,node3:9092");
        properties.put("metadata.broker.list", "node1:9092,node2:9092,node3:9092");
        //键的序列化器
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //消息的序列化器
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        //value的序列化器
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        /**
         *  0 : 这意味着生产者永远不会等待来自代理的确认（与0。7相同的行为）。这个选项提供了最低的延迟，但是最弱的持久性保证（当服务器失败时，一些数据会丢失）。
         *  1 : 在前导副本接收到数据之后，生产者获得确认。该选项提供了更好的持久性，因为客户机一直等待服务器确认请求成功（只有那些写入到已死的领导者的消息将丢失）。
         * -1 : 在所有同步副本都接收到数据之后，生产者得到了确认。这个选项提供了最好的持久性，我们保证只要至少一个同步副本保留，就不会丢失消息
         */
        properties.put("request.required.acks", "1");
        //指定分区
        /**
         * <li>If a partition is specified in the record, use it
         * <li>If no partition is specified but a key is present choose a partition based on a hash of the key
         * <li>If no partition or key is present choose a partition in a round-robin fashion
         */
        properties.put("partitioner.class", "kafka.producer.SimplePartitioner");
        //指定topic的分区数
        properties.put("num.pratitions", "3");

        producer = new KafkaProducer<Integer, String>(properties);
    }

    public void producerDemo() throws InterruptedException {
        int iCount = 100;
        while (true) {
            String message = "My Test Message No 00" + iCount;
            //下面的仅仅只发送value,如果要发送kv格式数据，
            //则使用： new ProducerRecord(topic,key,value);
            ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>("test01", message);
            Thread.sleep(1000);
            producer.send(record);
            iCount++;
        }
        //producer.close();
    }

    public static void main(String[] args) throws InterruptedException {

        new SimpleProducer().producerDemo();
    }
}
