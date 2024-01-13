package org.kafkaApp.Configuration;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class KafkaConfigLoader {

    public Properties loadProperties(String fileName) throws IOException {
        Properties props = new Properties();
        // Set default values
        setDefaultValues(props);

        // Load properties from file if it exists
        File configFile = new File(fileName);
        if (configFile.exists()) {
            try (FileInputStream fis = new FileInputStream(fileName)) {
                props.load(fis);
            }
        }
        return props;
    }
    private void setDefaultValues(Properties props) {
        // Set default configurations
        props.setProperty("linger.ms", "1000");
        props.setProperty("batch.size", "16384");
        props.setProperty("retries", "3");
        props.setProperty("acks", "all");
        props.setProperty("fetch.min.bytes", "2048");
        props.setProperty("fetch.max.wait.ms", "500");
        props.setProperty("max.request.size", "4097152");
        props.setProperty("max.block.ms", "120000");
        props.setProperty("delivery.timeout.ms", "120000");
        props.setProperty("buffer.memory", "67108864");
        // Add any other default properties here
    }
}
