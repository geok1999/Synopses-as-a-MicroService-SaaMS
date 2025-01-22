package org.kafkaApp.Metrics.collectorsJMX;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public class CommunicationCostJMXMetrics extends JMXMetrics{

    @Override
    public void collectMetrics() throws Exception {
        double totalSensorValue1 = 0;
        double totalSensorValue2 = 0;

        ObjectName mbeanName1 = new ObjectName("kafka.streams:type=finalSynopses-byte-countbyte-counting");
        ObjectName mbeanName2 = new ObjectName("kafka.streams:type=InitSynopses-byte-countbyte-counting");

        int i = 0;
        for (JMXServiceURL url : serviceUrls) {
            try (JMXConnector jmxc = JMXConnectorFactory.connect(url, null)) {
                MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
                i++;

                Double sensorValue1 = (Double) mbsc.getAttribute(mbeanName1, "finalSynopses-byte-count");
                Double sensorValue2 = (Double) mbsc.getAttribute(mbeanName2, "InitSynopses-byte-count");

                totalSensorValue1 += sensorValue1;
                totalSensorValue2 += sensorValue2;
            } catch (Exception e) {
                System.err.println("Failed to connect or retrieve metrics from URL: " + url);
                e.printStackTrace();
            }
        }

        // Print aggregated results
        System.out.println("Sensor Value for " + i + " instances of " + mbeanName1 + ": " + totalSensorValue1);
        System.out.println("Sensor Value for " + mbeanName2 + ": " + totalSensorValue2);
    }
}
