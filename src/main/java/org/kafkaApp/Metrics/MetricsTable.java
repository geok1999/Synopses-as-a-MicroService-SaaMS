package org.kafkaApp.Metrics;

public class MetricsTable {
    public final double throughput;
    public final double avgLatency;
    public final double maxLatency;

    public MetricsTable(double throughput, double avgLatency, double maxLatency) {
        this.throughput = throughput;
        this.avgLatency = avgLatency;
        this.maxLatency = maxLatency;
    }
}
