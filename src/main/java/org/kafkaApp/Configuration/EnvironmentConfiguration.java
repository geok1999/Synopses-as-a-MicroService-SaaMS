package org.kafkaApp.Configuration;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class EnvironmentConfiguration {
    private static final Properties properties = new Properties();
    private static final String Default_Location_DIR_PATH = "/home/gkalfakis/Configuration/configCluster.properties";

    static {
        String final_Location_Path = System.getProperty("configFilePath");

        if (final_Location_Path == null) {
            final_Location_Path = Default_Location_DIR_PATH;
        }

        try (InputStream input = new FileInputStream(final_Location_Path)) {
            properties.load(input);
        } catch (IOException e) {
            e.printStackTrace();
            // Handle exception, e.g., by falling back to default values or terminating the application.
        }
    }

    public static String getBootstrapServers() {
        return properties.getProperty("BOOTSTRAP_SERVERS", "localhost:9092,localhost:9093,localhost:9094");  // Default value is used if property is not found
    }

    public static String getFilePathPrefix() {
        return properties.getProperty("SAVE_FILE_PATH_PREFIX","C:\\dataset\\StoreSynopses\\stored_");
    }


    public static String getTempDir() {
        return properties.getProperty("KAFKA_STREAM_DIR","C:\\dataset\\tmp\\kafka-streams\\");
    }
    public static String getZookeeperBoostrapServer() {
        return properties.getProperty("ZOOKEEPER_BOOTSTRAP_SERVERS","localhost:2181");
    }

    public static int giveTheParallelDegree() {
        return Integer.parseInt(properties.getProperty("PARALLEL_DEGREE","6"));
    }
    public static int giveTheDividerForParallelDegree() {
        return Integer.parseInt(properties.getProperty("DIVIDE_FOR_PARALLEL_DEGREE","1"));
    }

    public static int giveTheReplicationFactor() {
        return Integer.parseInt(properties.getProperty("REPLICATION_FACTOR","3"));
    }
    public static String getFilePathForDataTopic() {
        return properties.getProperty("DATA_TOPIC_PATH","C:\\dataset\\ProduceDataToDataTopic");
    }
    public static String getFilePathForRequestTopic() {
        return properties.getProperty("REQUEST_TOPIC_PATH","C:\\Request_small.json");
    }

    public static String getFilePathForPropertiesfile() {
        return properties.getProperty("SAVE_FILE_PATH_PROPERTIES","C:\\dataset\\Configuration\\kafka-config.properties");
    }
    public static String TimeWaitToDoAdd() {
        return properties.getProperty("Batching_Time","2");
    }

}
