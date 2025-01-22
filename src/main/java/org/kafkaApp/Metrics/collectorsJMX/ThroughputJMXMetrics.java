package org.kafkaApp.Metrics.collectorsJMX;

import org.kafkaApp.Metrics.utils.FunctionalitiesJMX;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXServiceURL;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Set;

public class ThroughputJMXMetrics extends JMXMetrics {
    protected static int period = 0;

    @Override
    public void collectMetrics() throws Exception {
        double totalThroughput = 0.0;
        double totalLatency = 0.0;

        for (JMXServiceURL url : serviceUrls) {
            MBeanServerConnection mbsc = FunctionalitiesJMX.connectToJMX(url);

            ObjectName queryName = new ObjectName("kafka.streams:type=stream-thread-metrics,thread-id=*");
            Set<ObjectName> mbeanNames = mbsc.queryNames(queryName, null);

            for (ObjectName mbean : mbeanNames) {
                totalThroughput += FunctionalitiesJMX.getAttribute(mbsc, mbean, "process-rate");
                totalLatency += FunctionalitiesJMX.getAttribute(mbsc, mbean, "process-latency-avg");
            }

        }
        String result = "Period: " + period + "-" + (period+STANDARD_PERIOD) + "\n"+
                "Total Process Rate: " + totalThroughput + "\n" +
                "Total Average Latency: " + totalLatency + "\n" ;
        period += STANDARD_PERIOD;

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(super.fileName+".txt", true))) {
            writer.write(result);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
