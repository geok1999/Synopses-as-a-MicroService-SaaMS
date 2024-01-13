package org.kafkaApp.InitProducer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.kafkaApp.Configuration.EnvironmentConfiguration;
import org.kafkaApp.Serdes.Init.DataStructure.DataStructureDeserializer;
import org.kafkaApp.Serdes.Init.ListDataStructure.ListDataStructureDeserializer;
import org.kafkaApp.Serdes.SynopsesSerdes.SynopsisAndParameters.SynopsisAndParametersDeserializer;
import org.kafkaApp.Structure.DataStructure;
import org.kafkaApp.Structure.SynopsisAndParameters;

import java.time.Duration;
import java.util.*;

public class DataStructureCounter {
    private final String bootstrapServers;
    private final String groupId;
    public DataStructureCounter(String bootstrapServers, String groupId) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;

    }
    public  long countDataStructurePartition(String topic,int partitionNumber){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DataStructureDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");  // Added this line
        long count = 0;
        try (KafkaConsumer<String, DataStructure> consumer = new KafkaConsumer<>(props)) {
            TopicPartition topicPartition = new TopicPartition(topic, partitionNumber);
            consumer.assign(Collections.singletonList(topicPartition));

            while (true) {
                ConsumerRecords<String, DataStructure> records = consumer.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) {
                    break;
                }
                count += records.count();
            }
        }
        return count;
    }
    public long countDataStructures(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DataStructureDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");  // Added this line

        long count = 0;
        try (KafkaConsumer<String, DataStructure> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));

            while (true) {
                ConsumerRecords<String, DataStructure> records = consumer.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) {

                    break;
                }
                count += records.count();
            }
        }
        return count;
    }

    public long consumeFromChangeLogTopic(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ListDataStructureDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");


        long count = 0;

        try (KafkaConsumer<String, List<DataStructure>> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));

            int maxAttempts = 3;
            int attempt = 0;
            while (attempt++ < maxAttempts) {
                ConsumerRecords<String, List<DataStructure>> records = consumer.poll(Duration.ofSeconds(1));
                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, List<DataStructure>> record : records) {
                        count += record.value().size();
                    }
                } else {
                    System.out.println("Records are empty after " + attempt + " attempts.");
                }
            }
        }

        return count;
    }

    public Map<String, Long> calculateSynopsisSizeByKeys( String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SynopsisAndParametersDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        Map<String, Long> keyToSizeMap = new HashMap<>();

        try (KafkaConsumer<String, SynopsisAndParameters> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));

            while (true) {
                ConsumerRecords<String, SynopsisAndParameters> records = consumer.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) {
                    break;
                }

                for (ConsumerRecord<String, SynopsisAndParameters> record : records) {
                    SynopsisAndParameters sap = record.value();
                    if (sap != null && sap.getSynopsis() != null) {
                        keyToSizeMap.put(record.key(), (long) sap.getSynopsis().size());
                    }
                }
            }
        }
        return keyToSizeMap;
    }

    private static final String MICROSERVICE_ID = "Testsynopsis"+1+"microservice1";
    private final static int replicateFactor = 3;  // Unused variable
    private static final String BOOTSTRAP_SERVERS = EnvironmentConfiguration.getBootstrapServers();

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        //DataStructureCounter counter4 = new DataStructureCounter(BOOTSTRAP_SERVERS, MICROSERVICE_ID, "Data_Topic");
        while (true) {
            System.out.println("Please select an option:");
            System.out.println("1. Count for Intermidiate_Multy_Thread_Tpic");
            System.out.println("2. Count for DataTopicSynopsis");
            System.out.println("3. Count for Data_Topic");
            System.out.println("4. Get count for custom topic name");
            System.out.println("5. Exit");

            int selection = scanner.nextInt();

            switch (selection) {
                case 1:
                    for (int topicnum = 1; topicnum <= 6; topicnum++) {

                        // Construct the topic name based on the current topic number
                        String topic = "Intermidiate_Multy_Thread_Tpic_" + topicnum;

                        // Instantiate the DataStructureCounter for the current topic
                        DataStructureCounter counter = new DataStructureCounter(BOOTSTRAP_SERVERS, MICROSERVICE_ID);

                        // Calculate the synopsis size by keys for the current topic
                        Map<String, Long> resultMap = counter.calculateSynopsisSizeByKeys(topic);

                        // Initialize the total size for the current topic
                        int totalSize = 0;

                        // Print the results for the current topic
                        for (Map.Entry<String, Long> entry : resultMap.entrySet()) {
                            System.out.println("Key: " + entry.getKey() + ", Size: " + entry.getValue());
                            totalSize += entry.getValue();
                        }

                        // Print the total size for the current topic
                        System.out.println("Total Size for topic " + topic + ": " + totalSize);
                        System.out.println("========================================="); // Separator for clarity
                    }
                    break;

                case 2:
                    for (int topicnum = 1; topicnum <= 6; topicnum++) {

                        // Construct the topic name based on the current topic number
                        String topic1 = "DataTopicSynopsis_" + topicnum;

                        // Instantiate the DataStructureCounter for the current topic
                        DataStructureCounter counter1 = new DataStructureCounter(BOOTSTRAP_SERVERS, MICROSERVICE_ID);
                        long totalCount = counter1.countDataStructures(topic1);
                        System.out.println("Total Size for topic " + topic1 + ": " + totalCount);

                    }
                    break;

                case 3:
                    String topic = "Data_Topic";
                    int partitionsToRead = 6;  // The number of partitions you want to read
                    DataStructureCounter counter2 = new DataStructureCounter(BOOTSTRAP_SERVERS, MICROSERVICE_ID);

                    for (int partition = 0; partition < partitionsToRead; partition++) {
                        long totalCount = counter2.countDataStructurePartition(topic, partition);
                        System.out.println("Total DataStructure values in partition " + partition + " of topic " + topic + ": " + totalCount);
                    }

                    break;
                case 4:
                    System.out.println("Enter the name of the topic:");
                    scanner.nextLine();  // Consume newline left-over from nextInt()
                    String customTopic = scanner.nextLine();
                    System.out.println("Counting records in topic " + customTopic + "...");
                    DataStructureCounter counter = new DataStructureCounter(BOOTSTRAP_SERVERS, MICROSERVICE_ID);

                    long totalCount = counter.consumeFromChangeLogTopic("Intermidiate_Multy_Thread_Tpic_"+customTopic);
                    System.out.println("Total records in changelog topic: " + totalCount);
                    break;
                case 5:
                    System.out.println("Exiting program.");
                    scanner.close();
                    return;

                default:
                    System.out.println("Invalid selection. Please try again.");
            }

            System.out.println("=========================================");
        }
    }

}
 /*int partitionToRead = 0;  // For example, if you want to read from partition 0
        int partitionToRead2 = 1;  // For example, if you want to read from partition 0
        int partitionToRead3 = 2;  // For example, if you want to read from partition 0
*/
//DataStructureCounter counter2 = new DataStructureCounter(BOOTSTRAP_SERVERS, MICROSERVICE_ID, topic);

        /*long totalCount2 = counter2.countDataStructurePartition(partitionToRead);
        long totalCount3 = counter2.countDataStructurePartition(partitionToRead2);
        long totalCount4 = counter2.countDataStructurePartition(partitionToRead3);*/



        /*System.out.println("Total DataStructure values in partition " + partitionToRead + " of topic " + topic + ": " + totalCount2);
        System.out.println("Total DataStructure values in partition " + partitionToRead2 + " of topic " + topic + ": " + totalCount3);
        System.out.println("Total DataStructure values in partition " + partitionToRead3+ " of topic " + topic + ": " + totalCount4);*/