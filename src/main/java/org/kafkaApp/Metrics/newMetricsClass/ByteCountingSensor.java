package org.kafkaApp.Metrics.newMetricsClass;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.kafkaApp.Serdes.Init.DataStructure.DataStructureSerializer;
import org.kafkaApp.Serdes.SynopsesSerdes.SynopsisAndParameters.SynopsisAndParametersSerializer;
import org.kafkaApp.Structure.entities.DataStructure;
import org.kafkaApp.Structure.dto.SynopsisAndParameters;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class ByteCountingSensor {
    private static final Map<String, ByteCountingSensor> instances = new HashMap<>();

    private final Sensor byteCountSensor;
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

    public long getTotalBytes() {
        return totalBytes.get();
    }


}