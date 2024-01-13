package org.kafkaApp.Configuration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;
import java.util.UUID;

public class CreateConfiguration {

    public Properties getPropertiesForMicroservice(String APPLICATION_ID , String BOOTSTRAP_SERVERS) {
       // KafkaConfigLoader configLoader = new KafkaConfigLoader();
       // try {

            Properties properties = new Properties();
            //properties = configLoader.loadProperties(EnvironmentConfiguration.getFilePathForPropertiesfile());
            properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID); // application id for kafka streams (in this name based the group id for kafka consumer)
            properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS); // kafka bootstrap server this contains the broker address and ports (in this case we have 3 brokers)
            //properties.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 2);  // Setting number of standby replicas to 1 because we want to run only two instance(n the number of replicas stands by and n+1 the instance that is running)
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // earliest means that the consumer will read from the beginning of the topic
            properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, EnvironmentConfiguration.giveTheReplicationFactor()); // replication factor 2 this means that we have 2 replicas for each partition and distributed alongside the brokers
            properties.put(StreamsConfig.STATE_DIR_CONFIG, EnvironmentConfiguration.getTempDir() + UUID.randomUUID().toString()); // state directory for storing the state of the kafka streams
           //ForTesting temporary disable
            properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
            return properties;

       // } catch (IOException e) {
       //     e.printStackTrace();
            // Handle exception
     //   }


        //properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");

/*
        //properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), 60000);
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG), 2000);
        properties.put(StreamsConfig.producerPrefix(ProducerConfig.LINGER_MS_CONFIG), 5);
*/


        //properties.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,(APPLICATION_ID+UUID.randomUUID()));
       // properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");

        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        //properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);



        //properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 5 * 1024 * 1024); // Set the maximum request size to 5 MB
        // Set the number of standby replicas for each partition
        //properties.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 2); // Replace 2 with the desired number
        //properties.put(StreamsConfig.STATE_DIR_CONFIG, createTempStateDirectory());


        //---------------------------------------------------------------//
        //properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        //properties.put(StreamsConfig.STATE_DIR_CONFIG, "C:\\dataset\\streams");
        //properties.put("cleanup.policy", "delete");
       // properties.put("delete.retention.ms", "86400000");
        //properties.put("message.max.bytes", "52428800"); // Set the desired maximum message size (in bytes)
        //properties.put("log.segment.bytes", "536870912"); // Set the desired log segment size (in bytes)

        //properties.put(StreamsConfig.STATE_DIR_CONFIG, "C:\\dataset\\streams");
        //properties.put(StreamsConfig.topicPrefix(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG), 2);
        //properties.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");
       // properties.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);



       // properties.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group-"+groupCount.getAndIncrement());



       //return null;
    }

    public Properties getPropertiesConfig(Class<?> valueSerializerClass) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", EnvironmentConfiguration.getBootstrapServers());
        properties.put("acks","all");
        properties.put("retries", 3);

        //for sending batxhes
        properties.put("batch.size", 16384);  // Set batch size to 16 KB
        //properties.put("linger.ms", 10);  // Wait up to 10 ms for additional messages
        //properties.put("buffer.memory", 10);  // Wait up to 10 ms for additional messages

        //temporary disable
        properties.put("compression.type", "gzip");  // Enable compression

        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", valueSerializerClass.getName());
        return properties;
    }

}
