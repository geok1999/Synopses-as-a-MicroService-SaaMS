package org.kafkaApp.Configuration;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class EnvironmentConfiguration {
    private static final Properties properties = new Properties();
    private static final String Location_DIR_PATH = "/home/gkalfakis/Configuration/configCluster.properties";

    static {
        try (InputStream input = new FileInputStream(Location_DIR_PATH)) {
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

    public static String getFilePathForPropertiesfile() {
        return properties.getProperty("SAVE_FILE_PATH_PROPERTIES","C:\\dataset\\Configuration\\kafka-config.properties");
    }
    public static String TimeWaitToDoAdd() {
        return properties.getProperty("Batching_Time","2");
    }
  //make a main
    public static void main(String[] args) {
        System.out.println(EnvironmentConfiguration.getBootstrapServers());
        System.out.println(EnvironmentConfiguration.getFilePathPrefix());
        System.out.println(EnvironmentConfiguration.getTempDir());
        System.out.println(EnvironmentConfiguration.getZookeeperBoostrapServer());
    }


}
