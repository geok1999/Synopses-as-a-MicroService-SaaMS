package org.kafkaApp.Metrics;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.streams.StreamsMetrics;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MetricsReporter {
    private static final StreamsMetrics streamsMetrics = null;
    private static final String stageName = null;
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public MetricsReporter(StreamsMetrics streamsMetrics, String stageName) {

    }

    public static void reportMetrics(String operationRateStreamThroughput, String streamThroughputScopeMetrics) {
        Map<MetricName, ? extends Metric> allMetrics = streamsMetrics.metrics();
        for (Map.Entry<MetricName, ? extends Metric> entry : allMetrics.entrySet()) {
            MetricName name = entry.getKey();

            if (operationRateStreamThroughput.equals(name.name()) && streamThroughputScopeMetrics.equals(name.group())) {
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
    public static void main(String[] args) {
        scheduler.scheduleAtFixedRate(() -> {

            reportMetrics("ingestionRouter" + "operation-rate-rate", "stream-" + "ingestionRouter" + "-metrics");
            reportMetrics("ThroughputSensor" + "operation-rate-rate", "stream-" + "ThroughputSensor" + "-metrics");

        }, 5, 60, TimeUnit.SECONDS);
    }

}
