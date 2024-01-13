package org.kafkaApp.Microservices;

import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class TotalProcessedDataSensor {
    private final AtomicLong totalRecords = new AtomicLong(0);
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    private final StreamsMetrics streamsMetrics;
    private final String stageName;

    public TotalProcessedDataSensor(ProcessorContext context, String stageName) {
        this.streamsMetrics = context.metrics();
        this.stageName = stageName;

        scheduler.scheduleAtFixedRate(this::reportTotal, 5, 30, TimeUnit.SECONDS);
    }

    private void reportTotal() {
        System.out.println("Total records processed for " + stageName + ": " + totalRecords.get());
    }

    // Call this method whenever a record is processed
    public void record() {
        totalRecords.incrementAndGet();
    }

    public void shutdown() {
        scheduler.shutdown();
    }

    public static <T> ValueTransformerSupplier<T, T> createProcessedDataSensor(String sensorName) {
        return new ValueTransformerSupplier<T, T>() {

            @Override
            public ValueTransformer<T, T> get() {
                return new ValueTransformer<T, T>() {

                    private ProcessorContext context;
                    private TotalProcessedDataSensor processedDataSensor;

                    @Override
                    public void init(ProcessorContext context) {
                        this.context = context;
                        this.processedDataSensor = new TotalProcessedDataSensor(context, sensorName);
                    }

                    @Override
                    public T transform(T value) {
                        processedDataSensor.record();
                        return value;
                    }

                    @Override
                    public void close() {
                        processedDataSensor.shutdown();
                    }
                };
            }
        };
    }
}
