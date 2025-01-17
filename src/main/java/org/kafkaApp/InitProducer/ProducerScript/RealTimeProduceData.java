package org.kafkaApp.InitProducer.ProducerScript;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.kafkaApp.Configuration.CreateConfiguration;
import org.kafkaApp.Configuration.EnvironmentConfiguration;
import org.kafkaApp.Serdes.Init.DataStructure.DataStructureSerializer;
import org.kafkaApp.Structure.entities.DataStructure;
import org.kafkaApp.Structure.entities.RequestStructure;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

public class RealTimeProduceData implements Runnable{
    private final String filePath;
    private final KafkaProducer<String, Object> kafkaProducer;
    private final String topic;
    private final int partition;
    public RealTimeProduceData(String topic,String filePath,Properties props,int partition) {
        this.kafkaProducer = new KafkaProducer<>(props);
        this.topic = topic;
        this.filePath = filePath;
        this.partition=partition;
    }
    public void processFile() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        // Assuming each JSON object is on a new line in the file
        List<String> lines = Files.readAllLines(Paths.get(filePath), StandardCharsets.UTF_8);
        int i=0;
        for (String line : lines) {
            Object data = mapper.readValue(line, DataStructure.class);  // Replace Object.class with your data class
            kafkaProducer.send(new ProducerRecord<>(topic,partition,getKey(data) , data));
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

    public void close() {
        kafkaProducer.close();
    }
    public static void main(String[] args) throws Exception {

        CreateConfiguration createConfiguration=new CreateConfiguration();
        //Properties properties = createConfiguration.getPropertiesConfig(DataStructureSerializer.class);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", EnvironmentConfiguration.getBootstrapServers());
        properties.put("acks","all");
        properties.put("retries", 3);

        //for sending batxhes
        properties.put("batch.size", 16384);  // Set batch size to 16 KB
        //properties.put("linger.ms", 10);  // Wait up to 10 ms for additional messages
        //properties.put("buffer.memory", 10);  // Wait up to 10 ms for additional messages

        //properties.put("compression.type", "gzip");  // Enable compression

        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", DataStructureSerializer.class);


        String subDataPath="C:\\dataset\\ForexStocks\\Forex·EURTRY·NoExpiry.json";
        String topicName1="Data_Topic";

        // Build the Producer multithreaded
       // CreateTopic createTopic = new CreateTopic();
       // createTopic.createMyTopic(topicName1, 1, 3);
        //the data path of the data, wanted to send to the kafka


        RealTimeProduceData realTimeProduceData = new RealTimeProduceData( topicName1,subDataPath,properties,0);
        realTimeProduceData.processFile();
        realTimeProduceData.close();  // Ensure all remaining messages are sent before exiting
    }

    @Override
    public void run() {
        try {
            processFile();
            close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
