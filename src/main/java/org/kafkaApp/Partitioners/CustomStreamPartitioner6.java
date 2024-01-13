package org.kafkaApp.Partitioners;

import org.apache.kafka.streams.processor.StreamPartitioner;
import org.kafkaApp.Structure.SynopsisAndParameters;

import java.util.concurrent.atomic.AtomicInteger;

public class CustomStreamPartitioner6 implements StreamPartitioner<String, SynopsisAndParameters> {
    private AtomicInteger nextPartitionIndex = new AtomicInteger(0);
    @Override
    public Integer partition(String topic, String key,  SynopsisAndParameters value, int numPartitions) {
        int partitionIndex = nextPartitionIndex.getAndIncrement() % numPartitions;
        if (nextPartitionIndex.get() >= numPartitions) {
            nextPartitionIndex.set(0);
        }
        return partitionIndex;
    }
}

