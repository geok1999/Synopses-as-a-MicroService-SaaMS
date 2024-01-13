package org.kafkaApp.Partitioners;

import org.apache.kafka.streams.processor.StreamPartitioner;
import org.kafkaApp.Structure.DataStructure;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class CustomStreamPartitioner3 implements StreamPartitioner<String, DataStructure> {
    private AtomicInteger nextPartitionIndex = new AtomicInteger(0);
    Map<String, Integer> partionCountManager = new HashMap<>();
    @Override
    public Integer partition(String topic, String key, DataStructure value, int numPartitions) {


        if(partionCountManager.get(value.getStreamID())==null){
            partionCountManager.put(value.getStreamID(),nextPartitionIndex.getAndIncrement());
        }
        int partitionIndex = partionCountManager.get(value.getStreamID());
        return partitionIndex;

    }
}
