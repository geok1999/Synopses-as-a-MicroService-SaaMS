package org.kafkaApp.Microservices.Router;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.kafkaApp.Configuration.CreateTopic;
import org.kafkaApp.Configuration.EnvironmentConfiguration;
import org.kafkaApp.Serdes.SynopsesSerdes.BloomFilter.BloomFilterSerde;
import org.kafkaApp.Serdes.SynopsesSerdes.CountMin.CountMinSerde;
import org.kafkaApp.Serdes.SynopsesSerdes.HyperLogLog.HyperLogLogSerde;
import org.kafkaApp.Serdes.SynopsesSerdes.SynopsesSerdes;
import org.kafkaApp.Structure.entities.RequestStructure;
import org.kafkaApp.Synopses.BloomFilter.BloomFilterSynopsis;
import org.kafkaApp.Synopses.CountMin;
import org.kafkaApp.Synopses.HyperLogLog.HyperLogLogSynopsis;
import org.kafkaApp.Synopses.Synopsis;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class LoadAndSaveSynopsis {
    private Properties properties4;
    private int replicateFactor;
    private final String  LoadCountMinTopicName = "LoadCountMinSynopses_Topic";
    private final String  LoadHyperLogLogTopicName = "LoadHyperLogLogSynopses_Topic";
    private final String  LoadBloomFilterTopicName = "LoadBloomFilterSynopses_Topic";
    private final String  DFTTopicName = "LoadDFTSynopses_Topic";
    private final String  LossyCountingtopicName = "LoadLossyCountingSynopses_Topic";
    private final String  StickySamplingtopicName = "LoadStickySamplingSynopses_Topic";
    private final String  AMSSketchtopicName = "LoadAMSSketchSynopses_Topic";
    private final String  GKQuantilestopicName = "LoadGKQuantilesSynopses_Topic";
    private final String  LSHtopicName = "LoadLSHSynopses_Topic";
    private final String  WindowSketchQuantilestopicName = "LoadWindowSketchQuantilesSynopses_Topic";

    public LoadAndSaveSynopsis(){
    }
    public LoadAndSaveSynopsis(Properties properties, int replicateFactor) {
        this.properties4 = properties;
        this.replicateFactor= replicateFactor;
        CreateTopic createTopic = new CreateTopic();
        createTopic.createMyCompactTopic(LoadCountMinTopicName, 1, replicateFactor);
        createTopic.createMyCompactTopic(LoadHyperLogLogTopicName, 1, replicateFactor);
        createTopic.createMyCompactTopic(LoadBloomFilterTopicName, 1, replicateFactor);
        createTopic.createMyCompactTopic(DFTTopicName, 1, replicateFactor);
        createTopic.createMyCompactTopic(LossyCountingtopicName, 1, replicateFactor);
        createTopic.createMyCompactTopic(StickySamplingtopicName, 1, replicateFactor);
        createTopic.createMyCompactTopic(AMSSketchtopicName, 1, replicateFactor);
        createTopic.createMyCompactTopic(GKQuantilestopicName, 1, replicateFactor);
        createTopic.createMyCompactTopic(GKQuantilestopicName, 1, replicateFactor);
        createTopic.createMyCompactTopic(GKQuantilestopicName, 1, replicateFactor);
        createTopic.createMyCompactTopic(LSHtopicName, 1, replicateFactor);
        createTopic.createMyCompactTopic(WindowSketchQuantilestopicName, 1, replicateFactor);
    }

    public KStream<String, RequestStructure> loadRequestedSynopsis(KStream<String, RequestStructure> requestStream) {
        KStream<String, RequestStructure> requestStream2 =requestStream
                .map(((key, value) -> {
                    try {
                        Path path = Paths.get(value.getParam()[1].toString());
                        byte[] serializedData = Files.readAllBytes(path);
                        String fileName = path.getFileName().toString();

                        Synopsis loadedSynopsis = null;
                        String loadTopicName = null;
                        if (fileName.contains("CountMin")) {
                            try (CountMinSerde countMinSerde = new CountMinSerde()) {
                                Deserializer<CountMin> deserializer = countMinSerde.deserializer();
                                loadedSynopsis = deserializer.deserialize("", serializedData);
                                System.out.println("--------------------------------------------------------------------------------------");
                                System.out.println("Loaded CountMin object from disk");
                                System.out.println("--------------------------------------------------------------------------------------");
                                loadTopicName = LoadCountMinTopicName;

                            }
                        } else if (fileName.contains("HyperLogLog")) {
                            try (HyperLogLogSerde hyperLogLogSerde = new HyperLogLogSerde()) {
                                Deserializer<HyperLogLogSynopsis> deserializer = hyperLogLogSerde.deserializer();
                                loadedSynopsis = deserializer.deserialize("", serializedData);
                                loadTopicName = LoadHyperLogLogTopicName;
                            }
                        } else if (fileName.contains("BloomFilter")) {
                            try (BloomFilterSerde bloomFilterSerde = new BloomFilterSerde()) {
                                Deserializer<BloomFilterSynopsis> deserializer = bloomFilterSerde.deserializer();
                                loadedSynopsis = deserializer.deserialize("", serializedData);
                                loadTopicName = LoadBloomFilterTopicName;
                            }
                     /*   } else if (fileName.contains("DFT")) {
                           /* try (BloomFilterSerde bloomFilterSerde = new BloomFilterSerde()) {
                                Deserializer<BloomFilterSynopsis> deserializer = bloomFilterSerde.deserializer();
                                loadedSynopsis = deserializer.deserialize("", serializedData);
                                loadTopicName = LoadBloomFilterTopicName;
                            }*/
                 /*       } else if (fileName.contains("LossyCounting")) {
                           /* try (BloomFilterSerde bloomFilterSerde = new BloomFilterSerde()) {
                                Deserializer<BloomFilterSynopsis> deserializer = bloomFilterSerde.deserializer();
                                loadedSynopsis = deserializer.deserialize("", serializedData);
                                loadTopicName = LoadBloomFilterTopicName;
                            }*/
                 /*       }
                        else if (fileName.contains("StickySampling")) {
                           /* try (BloomFilterSerde bloomFilterSerde = new BloomFilterSerde()) {
                                Deserializer<BloomFilterSynopsis> deserializer = bloomFilterSerde.deserializer();
                                loadedSynopsis = deserializer.deserialize("", serializedData);
                                loadTopicName = LoadBloomFilterTopicName;
                            }*/
                   /*     }
                        else if (fileName.contains("AMSSketch")) {
                           /* try (BloomFilterSerde bloomFilterSerde = new BloomFilterSerde()) {
                                Deserializer<BloomFilterSynopsis> deserializer = bloomFilterSerde.deserializer();
                                loadedSynopsis = deserializer.deserialize("", serializedData);
                                loadTopicName = LoadBloomFilterTopicName;
                            }*/
                   /*     }else if (fileName.contains("GKQuantiles")) {
                           /* try (BloomFilterSerde bloomFilterSerde = new BloomFilterSerde()) {
                                Deserializer<BloomFilterSynopsis> deserializer = bloomFilterSerde.deserializer();
                                loadedSynopsis = deserializer.deserialize("", serializedData);
                                loadTopicName = LoadBloomFilterTopicName;
                            }*/
                        } else {
                            throw new RuntimeException("Unknown synopsis type in file: " + fileName);
                        }


                        String[] splitParams = loadedSynopsis.getSynopsisDetails().split(",");
                        String streamID = splitParams[5];
                        String dataSetKey = splitParams[6];
                        int synopsisID = loadedSynopsis.getSynopsesID();
                        String field = splitParams[3];
                        int uid = Integer.parseInt(splitParams[4]);
                        String noOfP = splitParams[2];
                        int requstID = Integer.parseInt(splitParams[0]);

                        String newKey = streamID + "," + dataSetKey + "," + synopsisID + "," + field;

                        value.setStreamID(streamID);
                        value.setDataSetKey(dataSetKey);
                        value.setSynopsisID(loadedSynopsis.getSynopsesID());
                        value.setParam(new Object[]{"LOAD_REQUEST", field, "NotQueryable", loadedSynopsis.getSynopsisParameters()});
                        value.setNoOfP(Integer.parseInt(noOfP));
                        value.setUid(uid);
                        value.setRequestID(requstID);

                        try (KafkaProducer<String, Synopsis> kafkaProducerLoad = new KafkaProducer<>(properties4)) {
                            kafkaProducerLoad.send(new ProducerRecord<>(loadTopicName, streamID + "," + dataSetKey, loadedSynopsis)); //value1 is the data
                        }

                        return new KeyValue<>(newKey, value);
                    } catch (IOException e) {
                        throw new RuntimeException("Error reading Synopsis object from disk", e);
                    }
                }));

        return requestStream2;

    }

    public void saveSynopsisToFile(Synopsis synopsis, String keyOfSynopsis,String synopsisType) {
        String filename = EnvironmentConfiguration.getFilePathPrefix() + synopsisType+"_"+ keyOfSynopsis + ".ser";

        try (SynopsesSerdes synopsesSerdes = new SynopsesSerdes();
             FileOutputStream fileOut = new FileOutputStream(filename);
             DataOutputStream dataOut = new DataOutputStream(fileOut)) {

            Serializer<Synopsis> serializer = synopsesSerdes.serializer();
            byte[] serializedData = serializer.serialize("", synopsis);
            dataOut.write(serializedData);
            System.out.println("Synopsis object has been written to " + filename);


        } catch (IOException e) {
            // Log the error or re-throw it as a runtime exception
            throw new RuntimeException("Error writing Synopsis object to disk", e);
        }



    }

}
