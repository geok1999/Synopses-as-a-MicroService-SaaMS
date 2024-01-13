package org.kafkaApp.Partitioners;

import org.apache.kafka.streams.processor.StreamPartitioner;

import java.util.function.Function;

public class CustomGenericStreamPartitioner<T> implements StreamPartitioner<String, T> {
    private final Function<T, Integer> partitionExtractor;
    private final int totalPartitions;

    public CustomGenericStreamPartitioner(Function<T, Integer> partitionExtractor,int totalPartitions) {
        this.partitionExtractor = partitionExtractor;
        this.totalPartitions = totalPartitions;
    }

    @Override
    public Integer partition(String topic, String key, T value, int numPartitions) {
        int partition = partitionExtractor.apply(value);
        return partition % totalPartitions;
    }
}