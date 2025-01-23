package org.metrics;

import org.metrics.collectorsJMX.CommunicationCostJMXMetrics;
import org.metrics.collectorsJMX.JMXMetrics;
import org.metrics.collectorsJMX.ThroughputJMXMetrics;
import org.metrics.utils.ConfigMetrics;

import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MetricsUserInterface {


    public static void main(String[] args) {
        String jmxUrls = ConfigMetrics.giveTheURLs();
        JMXMetrics.STANDARD_PERIOD = ConfigMetrics.giveTheMetricsPeriod();


        ThroughputJMXMetrics throughputJMXMetrics = new ThroughputJMXMetrics();
        throughputJMXMetrics.fileName =ConfigMetrics.giveTheOutputFileName();

        CommunicationCostJMXMetrics communicationCostJMXMetrics = new CommunicationCostJMXMetrics();

        Arrays.stream(jmxUrls.split(",")).forEach(url -> {
            try {
                throughputJMXMetrics.addServiceUrl(url);
                communicationCostJMXMetrics.addServiceUrl(url);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        Scanner scanner = new Scanner(System.in);
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

        while (true) {
            System.out.println("\n=== Metrics Menu ===");
            System.out.println("1. Get Throughput");
            System.out.println("2. Get Communication Cost");
            System.out.println("3. Exit");
            System.out.print("Enter your choice: ");
            int choice = scanner.nextInt();
            switch (choice) {
                case 1:
                    executorService.scheduleAtFixedRate(() -> {
                        try {
                            System.out.println();
                            System.out.println("New Throughput Metric Captured!!!");
                            throughputJMXMetrics.collectMetrics();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }, 0, JMXMetrics.STANDARD_PERIOD, TimeUnit.SECONDS);
                    break;
                case 2:
                    try {
                        communicationCostJMXMetrics.collectMetrics();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    break;
                case 3:
                    executorService.shutdown();
                    scanner.close();
                    return;
                default:
                    System.out.println("Invalid choice! Please try again.");
            }
        }

    }
}
