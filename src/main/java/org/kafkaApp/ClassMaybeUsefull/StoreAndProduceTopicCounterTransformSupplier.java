package org.kafkaApp.ClassMaybeUsefull;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.kafkaApp.Configuration.CreateTopic;
import org.kafkaApp.Structure.entities.RequestStructure;
import org.kafkaApp.Structure.dto.StructureWithMetaDataParameters;

import java.util.Properties;


public class StoreAndProduceTopicCounterTransformSupplier implements ValueTransformerWithKeySupplier<String, StructureWithMetaDataParameters<RequestStructure>, StructureWithMetaDataParameters<RequestStructure>> {

    private int replicateFactor;
    private int TotalParalilismDegree;
    public StoreAndProduceTopicCounterTransformSupplier(int replicateFactor,int TotalParalilismDegree){
        this.replicateFactor = replicateFactor;
        this.TotalParalilismDegree = TotalParalilismDegree;
    }
    private static class  StoreAndProduceTopicCounterTransform implements ValueTransformerWithKey<String, StructureWithMetaDataParameters<RequestStructure>,  StructureWithMetaDataParameters<RequestStructure>> {
        private KeyValueStore<String, Long> stateStore;
        private int replicateFactor;
        private static final String Topic_StateStore_COUNTER_KEY = "topic-count";
        private final CreateTopic createTopic;
        private int TotalParalilismDegree;
        private int TotalTopicCounter=0;

        public StoreAndProduceTopicCounterTransform(int replicateFactor,int TotalParalilismDegree){
            createTopic = new CreateTopic();
            this.replicateFactor = replicateFactor;
            this.TotalParalilismDegree = TotalParalilismDegree;
        }
        @Override
        public void init(ProcessorContext context) {

            stateStore = context.getStateStore("topic-counter-state-store");
        }

        @Override
        public StructureWithMetaDataParameters<RequestStructure> transform(String readOnlyKey, StructureWithMetaDataParameters<RequestStructure> value) {

            if(value.getReq().getParam()[2].equals("NotQueryable") && value.getMetaData1()<=1 || value.getReq().getParam()[0].equals("LOAD_REQUEST") ) {
                Properties properties = new Properties();
                properties.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
                properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                properties.put("value.serializer", "org.apache.kafka.common.serialization.LongSerializer");
                properties.put("acks", "all");
                properties.put("retries", 3);
                properties.put("batch.size", 16384);
                properties.put("linger.ms", 1);
                properties.put("buffer.memory", 33554432);
                try (KafkaProducer<String, Long> kafkaProducer = new KafkaProducer<>(properties)) {
                    kafkaProducer.send(new ProducerRecord<>("TopicCounter", readOnlyKey, 1L));
                }
                Long currentCount = stateStore.get(Topic_StateStore_COUNTER_KEY);

                System.out.println("1ReqestID "+value.getReq().getRequestID()+" repartionNum "+value.getMetaData3()+" currentCount: "+currentCount);


                String reqTopicName = "ReqTopicSynopsis_" + currentCount;
                String dataTopicName = "DataTopicSynopsis_" + currentCount;
                createTopic.createMyTopic(reqTopicName, value.getReq().getNoOfP(), replicateFactor);
                createTopic.createMyTopic(dataTopicName, value.getReq().getNoOfP(), replicateFactor);
                value.setMetaData2(Math.toIntExact(currentCount));
                return value;
            }
            Long currentCount = stateStore.get(readOnlyKey);
            System.out.println("1ReqestIDQuarable "+value.getReq().getRequestID()+" repartionNum "+value.getMetaData3()+" currentCount: "+currentCount);
            value.setMetaData2(Math.toIntExact(currentCount));

            return value;

        }


        @Override
        public void close() {

        }

        }
        @Override
        public ValueTransformerWithKey<String, StructureWithMetaDataParameters<RequestStructure>, StructureWithMetaDataParameters<RequestStructure>> get() {
            return new StoreAndProduceTopicCounterTransform(this.replicateFactor,this.TotalParalilismDegree);
        }
    }