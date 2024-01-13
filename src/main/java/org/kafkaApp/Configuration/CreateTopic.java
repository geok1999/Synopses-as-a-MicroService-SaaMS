package org.kafkaApp.Configuration;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Properties;

/**
 * This class is for creating kafka topics with spesific parameters
 *
 * @author George Kalfakis
 */
public class CreateTopic {
    private static final String BOOTSTRAP_SERVERS = EnvironmentConfiguration.getBootstrapServers();
    /**
     * Create a Kafka topic with spesific given name, partition number and replication factor
     *
     * @param topicName The name of the topic to be created
     * @param partitions the number of partitions of the topic
     * @param replicationFactor how many replicas will have this topics
     */
    public void createMyTopic(String topicName,int partitions, int replicationFactor) {
        Properties properties = new Properties();
        properties.put( AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        setTopic(topicName, partitions, (short) replicationFactor, properties);
    }


    public void createMyCompactTopic(String topicName,int partitions, int replicationFactor) {
        Properties properties = new Properties();
        properties.put( AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put("cleanup.policy", "compact");

        setTopic(topicName, partitions, (short) replicationFactor, properties);
    }

    private void setTopic(String topicName, int partitions, short replicationFactor, Properties properties) {
        try (AdminClient admin = AdminClient.create(properties)) {
            NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);

            admin.createTopics(Collections.singleton(newTopic));

            System.out.println("Created topic: "+topicName);
        }catch (Exception e) {
            System.out.println("Error creating topic: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        CreateTopic topicCreator = new CreateTopic();

        //Creating Data_Topic with 10 partitions and replication factor 1
        topicCreator.createMyTopic("Data_Topic", 3, 3);

        //Creating Request_Topic with 5 partitions and replication factor 1
        topicCreator.createMyTopic("Request_Topic", 3, 3);

        //topicCreator.createMyCompactTopic("LoadSynopses_Topic", 1, 1);


    }
}