package org.kafkaApp.Metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.kafkaApp.Serdes.Init.DataStructure.DataStructureSerializer;
import org.kafkaApp.Serdes.SynopsesSerdes.SynopsisAndParameters.SynopsisAndParametersSerializer;
import org.kafkaApp.Structure.entities.DataStructure;
import org.kafkaApp.Structure.dto.SynopsisAndParameters;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class ByteCountingSensor {
    private static final Map<String, ByteCountingSensor> instances = new HashMap<>();

    private final Sensor byteCountSensor;
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final StreamsMetrics streamsMetrics;

    private final String stageName;
    private final AtomicLong totalBytes = new AtomicLong();

    private static boolean isTaskScheduled = false;


    public ByteCountingSensor(ProcessorContext context, String stageName) {
        this.streamsMetrics = context.metrics();
        this.stageName = stageName;

        byteCountSensor = streamsMetrics.addSensor(stageName, Sensor.RecordingLevel.INFO);

        // Add a metric to the sensor
        MetricName metricName = new MetricName(
                stageName,               // name
                stageName+"byte-counting",         // group
                "Counts the bytes",      // description
                new HashMap<>()          // tags
        );
        byteCountSensor.add(metricName, new CumulativeSum());
    }

    public static synchronized ByteCountingSensor getInstance(ProcessorContext context, String stageName) {
        if (!instances.containsKey(stageName)) {
            instances.put(stageName, new ByteCountingSensor(context, stageName));
        }
        return instances.get(stageName);
    }
    public void recordDataStructureType(DataStructure value) {
        byte[] bytes;
        try (DataStructureSerializer serializer = new DataStructureSerializer()) {
            bytes = serializer.serialize(null, value);
        }
        byteCountSensor.record(bytes.length);
        totalBytes.addAndGet(bytes.length);
    }

    public void record(SynopsisAndParameters value) {
        byte[] bytes;
        try (SynopsisAndParametersSerializer serializer = new SynopsisAndParametersSerializer()) {
            bytes = serializer.serialize(null, value);
        }
        byteCountSensor.record(bytes.length);
        totalBytes.addAndGet(bytes.length);
    }


    public void shutdown() {
        scheduler.shutdown();
    }

    public long getTotalBytes() {
        return totalBytes.get();
    }

    public static <T> ValueTransformerSupplier<T, T> createByteCountingSensor(String sensorName) {
        return new ValueTransformerSupplier<T, T>() {

            @Override
            public ValueTransformer<T, T> get() {
                return new ValueTransformer<T, T>() {

                    private ProcessorContext context;
                    private ByteCountingSensor byteCountSensor;

                    @Override
                    public void init(ProcessorContext context) {
                        this.context = context;
                        this.byteCountSensor = ByteCountingSensor.getInstance(context, sensorName);
                    }

                    @Override
                    public T transform(T value) {
                        if(value instanceof SynopsisAndParameters)
                            byteCountSensor.record((SynopsisAndParameters) value);
                        else
                            byteCountSensor.recordDataStructureType((DataStructure) value);
                        return value;
                    }

                    @Override
                    public void close() {
                        byteCountSensor.shutdown();
                    }
                };
            }
        };
    }
}