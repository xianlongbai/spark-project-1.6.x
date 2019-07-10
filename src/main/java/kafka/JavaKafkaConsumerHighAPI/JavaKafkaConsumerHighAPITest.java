package kafka.JavaKafkaConsumerHighAPI;

/**
 * Created by root on 2018/4/1.
 */
public class JavaKafkaConsumerHighAPITest {

    public static void main(String[] args) {
        String zookeeper = "node1:2181,node2:2181,node3:2181";
        String groupId = "group1";
        String topic = "test01";
        int threads = 4;

        JavaKafkaConsumerHighAPI example = new JavaKafkaConsumerHighAPI(topic, threads, zookeeper, groupId);
        new Thread(example).start();

        // 执行100秒后结束
        int sleepMillis = 6000000;
        try {
            Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // 关闭
        example.shutdown();
    }
}
