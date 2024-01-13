package org.kafkaApp.Microservices.SynopseMicroservice;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.kafkaApp.Configuration.CreateConfiguration;
import org.kafkaApp.Configuration.EnvironmentConfiguration;
import org.kafkaApp.Serdes.Init.DataStructure.DataStructureSerde;
import org.kafkaApp.Serdes.Init.RequestStructure.RequestStructureSerde;
import org.kafkaApp.Structure.DataStructure;
import org.kafkaApp.Structure.RequestStructure;

import java.util.Properties;

public class RawDataMicroService {
    private KafkaStreams kafkaStreams;

    public RawDataMicroService(int topicCount, int numThreads) {
        final String MICROSERVICE_ID = "synopsis"+topicCount+"microservice";
        final String reqTopicName = "ReqTopicSynopsis"+"_"+topicCount;
        final String dataTopicName = "DataTopicSynopsis"+"_"+topicCount;


        Properties properties = new CreateConfiguration().getPropertiesForMicroservice(MICROSERVICE_ID, EnvironmentConfiguration.getBootstrapServers());
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numThreads / EnvironmentConfiguration.giveTheDividerForParallelDegree());
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "548576"); // Adjust as needed

        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "8384"); //16384 Example: 16KB batch size
        properties.put(ProducerConfig.RETRIES_CONFIG, 3); // Example: 3 retries

        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        //properties.put(ProducerConfig.LINGER_MS_CONFIG, "100"); // Example: 100 ms linger
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1048"); //2048 Example: 1KB minimum fetch
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "200"); //500 Example: 500 ms maximum wait
        properties.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_REQUEST_SIZE_CONFIG), "2097152"); //4097152 2MB

        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "120000"); // Increase to 120000 ms
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000"); // Increase to 120000 ms
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "67108864"); // Increase buffer memory to 32MB
        properties.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, CustomRocksDBConfig.class.getName());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, RequestStructure> requestStream = builder.stream(reqTopicName, Consumed.with(Serdes.String(), new RequestStructureSerde()));

        // Modify and add your processing logic for raw data here
        KStream<String, String> rawDataResult = requestStream
                .mapValues(value -> {
                    // Your processing logic for raw data
                    return "Processed Raw Data";
                });
        KStream<String, DataStructure> dataStream = builder.stream(dataTopicName, Consumed.with(Serdes.String(), new DataStructureSerde()));


        dataStream//.transform(()->new TotalInputTopicTransformer("InitSynopses-byte-count","RawData"))
                .to("Intermidiate_Multy_Thread_Tpic_"+topicCount , Produced.with(Serdes.String(), new DataStructureSerde()));

        KStream<String, DataStructure> subSynopses = builder.stream("Intermidiate_Multy_Thread_Tpic_"+topicCount, Consumed.with(Serdes.String(), new DataStructureSerde()))
                .transform(()->new TotalInputTopicTransformer("InitSynopses-byte-count","RawData"));

        kafkaStreams = new KafkaStreams(builder.build(), properties);
    }
    public void start() {
        kafkaStreams.start();
    }

    public void clear() {
        kafkaStreams.cleanUp();
    }
}
