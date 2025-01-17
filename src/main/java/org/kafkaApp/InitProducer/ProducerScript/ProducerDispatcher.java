package org.kafkaApp.InitProducer.ProducerScript;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.kafkaApp.Structure.entities.DataStructure;
import org.kafkaApp.Structure.entities.RequestStructure;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class ProducerDispatcher implements Runnable{

    private final String topicName;
    private final String fileLocation;
    private final Class<?> valueType;
    private final Properties props;
    private final int partitioner;
    public ProducerDispatcher(Properties props, String topicName, String fileLocation, Class<?> valueType, int partitioner) {
        this.topicName = topicName;
        this.fileLocation = fileLocation;
        this.valueType = valueType;
        this.props = props;
        this.partitioner = partitioner;
    }

    @Override
    public void run() {
        ObjectMapper mapper = new ObjectMapper();
        try (KafkaProducer<String, List<?>> producer = new KafkaProducer<>(this.props)) {
            List<?> elementsList = mapper.readValue(new File(this.fileLocation), mapper.getTypeFactory().constructCollectionType(List.class, valueType));

            int batchSize = 50;

            // Partition the list into sublist for more efficient
            Map<Object, ? extends List<?>> partitions = elementsList.parallelStream()
                    .collect(Collectors.groupingBy(s -> elementsList.indexOf(s) / batchSize));

            // Get the list of sublist
            List<List<?>> subLists = new ArrayList<>(partitions.values());


            // Make a stream of records and send them to the Kafka topic
            subLists.parallelStream().forEachOrdered(sublist -> {
                String key = getKey(sublist.get(0));
                if (key.contains("Request")) {
                    // Send to specific partition (1 in this case)
                    ProducerRecord<String, List<?>> kafkaRecord = new ProducerRecord<>(topicName, 0, key, sublist);
                    producer.send(kafkaRecord);
                } else {
                    System.out.println("key: "+key);
                    ProducerRecord<String, List<?>> kafkaRecord = new ProducerRecord<>(topicName, this.partitioner, key, sublist);
                    producer.send(kafkaRecord);
                }
            });

        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            System.err.println("Finished dispatcher demo - Closing Kafka Producer.");
        }
    }

    private String getKey(Object obj) {
        if (obj instanceof DataStructure) {
            return ((DataStructure) obj).getStreamID()+","+((DataStructure) obj).getDataSetKey();
        } else if (obj instanceof RequestStructure) {
            return "Request";
        }
        throw new IllegalArgumentException("Unsupported object type");
    }
}
