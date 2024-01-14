package org.kafkaApp.Metrics;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class JMXCommunicationCostCollector {
    private List<JMXServiceURL> urls = new ArrayList<>();
    private static JMXCommunicationCostCollector collector = new JMXCommunicationCostCollector();
    public void addUrl(String url) throws Exception {
        urls.add(new JMXServiceURL(url));
    }


    public static void manageServiceUrls() {

        int choice = -1;
        while (choice != 0) {
            Scanner scanner = new Scanner(System.in);
            System.out.println("Enter 1 to add a URL or 0 to stop this process:");
            choice = scanner.nextInt();
            scanner.nextLine();  // Consume newline left-over
            if (choice == 1) {
                System.out.println("Enter the URL:");
                String url = scanner.nextLine();
                try {
                    collector.addUrl(url);
                    System.out.println("URL added successfully.");
                } catch (Exception e) {
                    System.out.println("Invalid URL. Please enter a valid JMXServiceURL.");
                }
            } else if (choice != 0) {
                System.out.println("Invalid choice.");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        //JMXCommunicationCostCollector collector = new JMXCommunicationCostCollector();
        //service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi
        // Add URLs as needed
       // collector.addUrl("service:jmx:rmi:///jndi/rmi://snf-36110.ok-kno.grnetcloud.net:9999/jmxrmi");
        collector.addUrl("service:jmx:rmi:///jndi/rmi://polytechnix.softnet.tuc.gr:9999/jmxrmi");
        manageServiceUrls();
        // Add more URLs as needed
        // urls.add(new JMXServiceURL("service:jmx:rmi:///jndi/rmi://another-host:another-port/jmxrmi"));
        //ObjectName mbeanNames = new ObjectName("kafka.streams:type=finalSynopses-byte-countbyte-counting");
        ObjectName mbeanNames2 = new ObjectName("kafka.streams:type=InitSynopses-byte-countbyte-counting");

        double totalSensorValue1 = 0;
        double totalSensorValue2 = 0;
        int i=0;
        for (JMXServiceURL url : collector.urls) {
            try (JMXConnector jmxc = JMXConnectorFactory.connect(url, null)) {
                MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
                i++;
                // Construct the ObjectName for the MBean you want to access


              // Double sensorValue1 = (Double) mbsc.getAttribute(mbeanNames, "finalSynopses-byte-count");
                Double sensorValue2 = (Double) mbsc.getAttribute(mbeanNames2, "InitSynopses-byte-count");
               // totalSensorValue1 += sensorValue1;
                totalSensorValue2 += sensorValue2;

            }
        }
        //System.out.println("Sensor Value for " +collector.urls.size()+ mbeanNames + ": " + totalSensorValue1);
        System.out.println("Sensor Value for " + mbeanNames2 + ": " + totalSensorValue2);
    }
}