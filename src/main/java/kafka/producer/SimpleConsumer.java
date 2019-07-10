package kafka.producer;

import kafka.consumer.Consumer;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by root on 2018/4/1.
 *
 *
 */
public class SimpleConsumer{

    private final  ConsumerConnector consumer;
    private final static String Topic = "test01";

    public SimpleConsumer(){
        Properties props = new Properties();
        // zookeeper 配置
        props.put("zookeeper.connect", "node2:2181,node3:2181,node4:2181");
        // group 代表一个消费组
        props.put("group.id", "jd-group");
        // zk连接超时
        props.put("zookeeper.session.timeout.ms", "4000");
        //ZK的追随者可以在ZK领导的后面走多远
        props.put("zookeeper.sync.time.ms", "200");
        //提交间隔
        props.put("auto.commit.interval.ms", "1000");
        /**
         * What to do when there is no initial offset in ZooKeeper or if an offset is out of range:
         * smallest : automatically reset the offset to the smallest offset
         * largest : automatically reset the offset to the largest offset
         * anything else: throw exception to the consumer
         */
        props.put("auto.offset.reset", "smallest");
        // 序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //构建consumer connection 对象
        consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
    }

    public void consume(){
        //指定需要订阅的topic
        Map<String ,Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(Topic, new Integer(5));

        //指定key的编码格式
        Decoder<String> keyDecoder = new kafka.serializer.StringDecoder(new VerifiableProperties());
        //指定value的编码格式
        Decoder<String> valueDecoder = new kafka.serializer.StringDecoder(new VerifiableProperties());
        //获取topic 和 接受到的stream 集合
        Map<String, List<KafkaStream<String, String>>> map = consumer.createMessageStreams(topicCountMap, keyDecoder, valueDecoder);
        //根据指定的topic 获取 stream 集合
        List<KafkaStream<String, String>> kafkaStreams = map.get(Topic);
        ExecutorService executor = Executors.newFixedThreadPool(4);

        //因为是多个 message组成 message set ， 所以要对stream 进行拆解遍历
        for(final KafkaStream<String, String> kafkaStream : kafkaStreams){
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    //拆解每个的 stream
                    ConsumerIterator<String, String> iterator = kafkaStream.iterator();
                    while (iterator.hasNext()) {
                        //messageAndMetadata 包括了 message ， topic ， partition等metadata信息
                        MessageAndMetadata<String, String> messageAndMetadata = iterator.next();
                        System.out.println("message : " + messageAndMetadata.message() + "  partition :  " + messageAndMetadata.partition() +
                        " topic : " + messageAndMetadata.topic() + " offset :" + messageAndMetadata.offset());

                    }
                }
            });


        }
    }



    public static void main(String[] args){
        new SimpleConsumer().consume();
    }



}
