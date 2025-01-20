package org.kafkaApp.Metrics;

import org.kafkaApp.Metrics.newMetricsClass.FunctionalitiesJMX;

import javax.management.AttributeNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXServiceURL;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class JMXMetricsCollector {
    private static final int STANDARD_PERIOD=5;
    private static int period = 0;
    private static List<JMXServiceURL> serviceUrls = new ArrayList<>();


    public static void main(String[] args) throws Exception {


        System.out.println("Configured JMX Service URLs:");

        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter the case name:");
        String fileName = scanner.nextLine();

        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();


        //addServiceUrl("service:jmx:rmi:///jndi/rmi://snf-36110.ok-kno.grnetcloud.net:9999/jmxrmi");
        //addServiceUrl("service:jmx:rmi:///jndi/rmi://snf-36112.ok-kno.grnetcloud.net:9999/jmxrmi");
        //addServiceUrl("service:jmx:rmi:///jndi/rmi://snf-36103.ok-kno.grnetcloud.net:9999/jmxrmi");
        //addServiceUrl("service:jmx:rmi:///jndi/rmi://polytechnix.softnet.tuc.gr:9999/jmxrmi");
        addServiceUrl("service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi");

        //manageServiceUrls();

        // Schedule the task to run every 1 minute
        executorService.scheduleAtFixedRate(() -> {
            try {
                collectMetricsAndWriteToFile(fileName,period);
                period += STANDARD_PERIOD;

            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 0, STANDARD_PERIOD, TimeUnit.SECONDS);
    }

    public static void manageServiceUrls() throws Exception {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter 1 to add a URL or 2 to remove a URL or anything else to stop this process:");
        int choice = scanner.nextInt();
        scanner.nextLine();  // Consume newline left-over
        System.out.println("Enter the URL:");
        String url = scanner.nextLine();

        switch (choice) {
            case 1:
                addServiceUrl(url);
                System.out.println("URL added successfully.");
                break;
            case 2:
                removeServiceUrl(url);
                System.out.println("URL removed successfully.");
                break;
            default:
                System.out.println("Invalid choice.");
                break;
        }
    }

    public static void addServiceUrl(String url) throws Exception {
        serviceUrls.add(new JMXServiceURL(url));
    }

    public static void removeServiceUrl(String url) throws Exception {
        JMXServiceURL serviceURL = new JMXServiceURL(url);
        serviceUrls.remove(serviceURL);
    }
    private static void collectMetricsAndWriteToFile(String fileName, int period) throws Exception {
        //List<JMXServiceURL> serviceUrls = new ArrayList<>();

        double totalProcessRate = 0;
        double totalAvgLatency = 0;
        double totalMaxLatency = 0;

        for (JMXServiceURL url : serviceUrls) {
            MBeanServerConnection mbsc = FunctionalitiesJMX.connectToJMX(url);

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
        }
        String runId = UUID.randomUUID().toString();

        String result = "Period: " + period + "-" + (period+STANDARD_PERIOD) + "\n"+
                "Total Process Rate: " + totalProcessRate + "\n" +
                "Total Average Latency: " + totalAvgLatency + "\n" +
                "Total Max Latency: " + totalMaxLatency + "\n"+ "\n";

        // Write the result to a file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("C:\\dataset\\MetricsResults\\"+fileName+".txt", true))) {
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

                //System.out.println("Process Rate for " + mbeanName + ": " + processRate);
                //System.out.println("Average Latency for " + mbeanName + ": " + avgLatency);
                //System.out.println("Max Latency for " + mbeanName + ": " + maxLatency);
            } catch (AttributeNotFoundException e) {
                System.out.println("Attribute not found for " + mbeanName + ": " + e.getMessage());
            }
        }
        return  new MetricsTable(aggregatedRate, aggregatedAvgLatency, aggregatedMaxLatency);
    }
}