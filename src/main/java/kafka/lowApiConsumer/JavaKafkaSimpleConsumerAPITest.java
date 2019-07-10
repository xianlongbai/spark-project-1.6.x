package kafka.lowApiConsumer;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by root on 2018/4/1.
 *
 * 改代码只能接收指定的某个partition消息
 */
public class JavaKafkaSimpleConsumerAPITest {


    public static void main(String[] args){
        JavaKafkaSimpleConsumerAPI example = new JavaKafkaSimpleConsumerAPI();
        long maxReads = 5000;
        String topic = "test01";
        int partitionID = 0;

        KafkaTopicPartitionInfo topicPartitionInfo = new KafkaTopicPartitionInfo(topic, partitionID);
        List<KafkaBrokerInfo> seeds = new ArrayList<KafkaBrokerInfo>();
        seeds.add(new KafkaBrokerInfo("node1", 9092));
        seeds.add(new KafkaBrokerInfo("node2", 9092));
        seeds.add(new KafkaBrokerInfo("node3", 9092));

        try {
            example.run(maxReads, topicPartitionInfo, seeds);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 获取该topic所属的所有分区ID列表
        System.out.println(example.fetchTopicPartitionIDs(seeds, topic, 100000, 64 * 1024, "client-id"));
    }


}
