package org.kafkaApp.Partitioners;

import org.apache.kafka.streams.processor.StreamPartitioner;
import org.kafkaApp.Structure.entities.DataStructure;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class CustomStreamPartitioner5 implements StreamPartitioner<String, List<DataStructure>> {
    private AtomicInteger nextPartitionIndex = new AtomicInteger(0);
    @Override
    public Integer partition(String topic, String key,  List<DataStructure> value, int numPartitions) {
        int partitionIndex = nextPartitionIndex.getAndIncrement() % numPartitions;
        if (nextPartitionIndex.get() >= numPartitions) {
            nextPartitionIndex.set(0);
        }
        return partitionIndex;
    }
}

