package kafka.producer;

import kafka.utils.VerifiableProperties;

/**
 * Created by root on 2018/4/1.
 */
public class SimplePartitioner implements Partitioner {

    public SimplePartitioner (VerifiableProperties props) {

    }

    @Override
    public int partition(Object key, int numPartitions) {
        int partition = 0;
        String stringKey = (String) key;
        int index = stringKey.lastIndexOf(':');
        if (index > 0) {
            partition = Integer.parseInt( stringKey.substring(index+1)) % numPartitions;
        }
        return partition;
    }
}
