package org.kafkaApp.Partitioners;

import org.apache.kafka.streams.processor.StreamPartitioner;
import org.kafkaApp.Configuration.EnvironmentConfiguration;
import org.kafkaApp.Structure.DataStructure;

import java.util.concurrent.atomic.AtomicInteger;

public class CustomStreamPartitioner4 implements StreamPartitioner<String, DataStructure> {
    private AtomicInteger nextPartitionIndex = new AtomicInteger(0);

    @Override
    public Integer partition(String topic, String key, DataStructure value, int numPartitions) {
        int partitionIndex = nextPartitionIndex.getAndIncrement() % EnvironmentConfiguration.giveTheParallelDegree();
        if (nextPartitionIndex.get() >= numPartitions) {
            nextPartitionIndex.set(0);
        }
        return partitionIndex;
    }
}
