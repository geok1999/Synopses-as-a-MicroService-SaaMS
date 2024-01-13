package org.kafkaApp.Metrics;

import javax.management.AttributeNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class JMXMetricsCollector {

    public static void main(String[] args) throws Exception {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

        // Schedule the task to run every 1 minute
        executorService.scheduleAtFixedRate(() -> {
            try {
                collectMetricsAndWriteToFile();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 0, 10, TimeUnit.SECONDS);
    }

    private static void collectMetricsAndWriteToFile() throws Exception {
        List<JMXServiceURL> serviceUrls = new ArrayList<>();
        serviceUrls.add(new JMXServiceURL("service:jmx:rmi:///jndi/rmi://snf-36110.ok-kno.grnetcloud.net:9999/jmxrmi"));
        serviceUrls.add(new JMXServiceURL("service:jmx:rmi:///jndi/rmi://snf-36112.ok-kno.grnetcloud.net:9999/jmxrmi"));

        double totalProcessRate = 0;
        double totalAvgLatency = 0;
        double totalMaxLatency = 0;

        for (JMXServiceURL url : serviceUrls) {
            JMXConnector jmxc = null;
            try {
                jmxc = JMXConnectorFactory.connect(url, null);
                MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();

                ObjectName queryNameRouter = new ObjectName("kafka.streams:type=stream-thread-metrics,thread-id=router-microservice-*");
                ObjectName queryNameSynopsis = new ObjectName("kafka.streams:type=stream-thread-metrics,thread-id=synopsis*microservice-*");

                Set<ObjectName> mbeanNamesRouter = mbsc.queryNames(queryNameRouter, null);
                Set<ObjectName> mbeanNamesSynopsis = mbsc.queryNames(queryNameSynopsis, null);

                // Aggregating throughput for Router Microservice
                MetricsTable routerMetrics = aggregateThroughput(mbsc, mbeanNamesRouter);
                totalProcessRate += routerMetrics.throughput;
                totalAvgLatency += routerMetrics.avgLatency;
                totalMaxLatency = Math.max(totalMaxLatency, routerMetrics.maxLatency);

                // Aggregating throughput for Synopses Microservices
                MetricsTable synopsisMetrics = aggregateThroughput(mbsc, mbeanNamesSynopsis);
                totalProcessRate += synopsisMetrics.throughput;
                totalAvgLatency += synopsisMetrics.avgLatency;
                totalMaxLatency = Math.max(totalMaxLatency, synopsisMetrics.maxLatency);
            } finally {
                if (jmxc != null) {
                    try {
                        jmxc.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        String runId = UUID.randomUUID().toString();

        String result = "Run ID: " + runId + "\n" +
                "Total Process Rate: " + totalProcessRate + "\n" +
                "Total Average Latency: " + totalAvgLatency + "\n" +
                "Total Max Latency: " + totalMaxLatency + "\n";

        // Write the result to a file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("C:\\dataset\\MetricsResults\\metrics.txt", true))) {
            writer.write(result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static MetricsTable aggregateThroughput(MBeanServerConnection mbsc, Set<ObjectName> mbeanNames) throws Exception {
        double aggregatedRate = 0;
        double aggregatedAvgLatency = 0;
        double aggregatedMaxLatency = 0;

        for (ObjectName mbeanName : mbeanNames) {
            try {
                Double processRate = (Double) mbsc.getAttribute(mbeanName, "process-rate");
                aggregatedRate += processRate;
                Double avgLatency = (Double) mbsc.getAttribute(mbeanName, "process-latency-avg");
                aggregatedAvgLatency += avgLatency;
                Double maxLatency = (Double) mbsc.getAttribute(mbeanName, "process-latency-max");
                aggregatedMaxLatency = Math.max(aggregatedMaxLatency, maxLatency);  // We take the maximum latency across all threads.

                System.out.println("Process Rate for " + mbeanName + ": " + processRate);
                //System.out.println("Average Latency for " + mbeanName + ": " + avgLatency);
                //System.out.println("Max Latency for " + mbeanName + ": " + maxLatency);
            } catch (AttributeNotFoundException e) {
                System.out.println("Attribute not found for " + mbeanName + ": " + e.getMessage());
            }
        }
        return  new MetricsTable(aggregatedRate, aggregatedAvgLatency, aggregatedMaxLatency);
    }
}
/*
 public static void main(String[] args) throws Exception {
        JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi");
        try (JMXConnector jmxc = JMXConnectorFactory.connect(url, null)) {
            MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();

            ObjectName queryNameRouter = new ObjectName("kafka.streams:type=stream-thread-metrics,thread-id=router-microservice-*");
            ObjectName queryNameSynopsis = new ObjectName("kafka.streams:type=stream-thread-metrics,thread-id=synopsis*microservice-*");

            Set<ObjectName> mbeanNamesRouter = mbsc.queryNames(queryNameRouter, null);
            Set<ObjectName> mbeanNamesSynopsis = mbsc.queryNames(queryNameSynopsis, null);

            CompletableFuture<Double> routerFuture = aggregateThroughputAsync(mbsc, mbeanNamesRouter);
            CompletableFuture<Double> synopsisFuture = aggregateThroughputAsync(mbsc, mbeanNamesSynopsis);

            double totalProcessRate = routerFuture.thenCombine(synopsisFuture, Double::sum).get();

            System.out.println("Total Process Rate: " + totalProcessRate);
        }
    }

    private static CompletableFuture<Double> aggregateThroughputAsync(MBeanServerConnection mbsc, Set<ObjectName> mbeanNames) {
        return CompletableFuture.supplyAsync(() -> {
            return mbeanNames.parallelStream().mapToDouble(mbeanName -> {
                try {
                    return (Double) mbsc.getAttribute(mbeanName, "process-rate");
                } catch (Exception e) {
                    System.out.println("Error fetching process rate for " + mbeanName + ": " + e.getMessage());
                    return 0.0;
                }
            }).sum();
        });
    }
 */