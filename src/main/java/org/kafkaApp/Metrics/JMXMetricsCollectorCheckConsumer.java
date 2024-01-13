package org.kafkaApp.Metrics;

import javax.management.AttributeNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class JMXMetricsCollectorCheckConsumer {
    public static void main(String[] args) throws Exception {
        List<JMXServiceURL> serviceUrls = new ArrayList<>();
        serviceUrls.add(new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi"));
        serviceUrls.add(new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:9998/jmxrmi"));

        List<Double> totalRecordsConsumed = new ArrayList<>();

        for (JMXServiceURL url : serviceUrls) {
            JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
            MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();

            // Use a specific thread ID for the Synopses Microservice you want to monitor.
            String desiredThreadID = "synopsis*microservice-*-consumer"; // Replace with the actual thread ID

            ObjectName queryNameConsumerFetchManager = new ObjectName("kafka.consumer:type=consumer-fetch-manager-metrics,client-id=" + desiredThreadID);
            Set<ObjectName> mbeanNamesSynopsis = mbsc.queryNames(queryNameConsumerFetchManager, null);

            Double recordsConsumedSynopsis = getRecordsConsumedTotal(mbsc, mbeanNamesSynopsis);
           // if (recordsConsumedSynopsis > 1.0) {
                totalRecordsConsumed.add(recordsConsumedSynopsis);
           // }
        }

        Double combinedRecordsConsumed = totalRecordsConsumed.stream().mapToDouble(Double::doubleValue).sum();
        System.out.println("Combined Total Records Consumed for Synopses Microservice: " + combinedRecordsConsumed);
    }

    private static Double getRecordsConsumedTotal(MBeanServerConnection mbsc, Set<ObjectName> mbeanNames) throws Exception {
        Double totalRecordsConsumed = 0.0;

        for (ObjectName mbeanName : mbeanNames) {
            if (!mbeanName.toString().contains("-restore-consumer")) {
                try {
                    Double recordsConsumed = (Double) mbsc.getAttribute(mbeanName, "records-consumed-total");
                   // if (recordsConsumed > 1.0) {
                        totalRecordsConsumed += recordsConsumed;
                        System.out.println("Records Consumed for " + mbeanName + ": " + recordsConsumed);
                  //  }
                } catch (AttributeNotFoundException e) {
                    System.out.println("Attribute not found for " + mbeanName + ": " + e.getMessage());
                }
            }
        }

        return totalRecordsConsumed;
    }
}