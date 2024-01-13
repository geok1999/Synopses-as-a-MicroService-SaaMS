package org.kafkaApp.Metrics;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class ThroughputSensor {
    private static final Map<String, ThroughputSensor> instances = new HashMap<>();

    private final Sensor throughputSensor;
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
   //private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final StreamsMetrics streamsMetrics;

    private final String stageName;

    private static boolean isTaskScheduled = false;  // A flag to check if task is already scheduled
    public ThroughputSensor(ProcessorContext context, String stageName) {
        this.streamsMetrics = context.metrics();
        this.stageName = stageName;

        // Define the sensor using addRateTotalSensor
        throughputSensor = streamsMetrics.addRateTotalSensor(
                stageName,     // scope name
                stageName+"entity",                    // entity name
                stageName+"operation-rate",           // operation name
                RecordingLevel.INFO        // recording level
                // ... you can add additional tags if required
        );

        reportMetrics(stageName+"operation-rate-rate","stream-"+stageName+"-metrics");

    }

    private void reportMetrics(String operationRateStreamThroughput,String StreamThroughputScopeMetrics) {

        Map<MetricName, ? extends Metric> allMetrics = streamsMetrics.metrics();
        for (Map.Entry<MetricName, ? extends Metric> entry : allMetrics.entrySet()) {
            MetricName name = entry.getKey();
           // System.out.println("name" + ": " +name);
            if (operationRateStreamThroughput.equals(name.name()) && StreamThroughputScopeMetrics.equals(name.group())) {
                double rate = (double) entry.getValue().metricValue();
                try (BufferedWriter writer = new BufferedWriter(new FileWriter("C:\\dataset\\MetricsResults\\throughput_data.txt", true))) {
                    writer.write("Throughput: " + stageName + "," + rate + " records/second\n");
                } catch (IOException e) {
                    e.printStackTrace();
                }
                break;
            }
        }
    }
    public static synchronized ThroughputSensor getInstance(ProcessorContext context, String stageName) {
        if (!instances.containsKey(stageName)) {
            instances.put(stageName, new ThroughputSensor(context, stageName));
        }
        return instances.get(stageName);
    }

    // Call this method whenever a record is processed
    public void record() {
        throughputSensor.record(1.0);
    }

    public void shutdown() {
        scheduler.shutdown();
    }

    public static <T> ValueTransformerSupplier<T, T> createThroughputSensor(String sensorName) {
        return new ValueTransformerSupplier<T, T>() {

            @Override
            public ValueTransformer<T, T> get() {
                return new ValueTransformer<T, T>() {

                    private ProcessorContext context;
                    private ThroughputSensor throughputSensor;

                    @Override
                    public void init(ProcessorContext context) {
                        this.context = context;
                        this.throughputSensor = ThroughputSensor.getInstance(context, sensorName);
                    }

                    @Override
                    public T transform(T value) {
                        throughputSensor.record();
                        // Your transformation logic here
                        return value;
                    }

                    @Override
                    public void close() {
                        throughputSensor.shutdown();
                    }
                };
            }
        };
    }

}

