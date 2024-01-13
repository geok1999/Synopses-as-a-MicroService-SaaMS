package org.kafkaApp.Microservices.Router;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.kafkaApp.Configuration.CreateConfiguration;
import org.kafkaApp.Configuration.CreateTopic;
import org.kafkaApp.Configuration.EnvironmentConfiguration;
import org.kafkaApp.Microservices.SynopseMicroservice.GenericSynopsesMicroService;
import org.kafkaApp.Microservices.SynopseMicroservice.RawDataMicroService;
import org.kafkaApp.Partitioners.CustomGenericStreamPartitioner;
import org.kafkaApp.Partitioners.CustomStreamPartitioner4;
import org.kafkaApp.Serdes.Init.DataStructure.DataStructureSerde;
import org.kafkaApp.Serdes.Init.DataStructure.DataStructureSerializer;
import org.kafkaApp.Serdes.Init.ListRequestStructure.ListRequestStructureSerde;
import org.kafkaApp.Serdes.Init.ListRequestStructure.ListRequestStructureSerializer;
import org.kafkaApp.Serdes.Init.RequestStructure.RequestStructureSerde;
import org.kafkaApp.Serdes.Init.RequestWithMetaDataParameters.RequestWithMetaDataParametersSerde;
import org.kafkaApp.Structure.DataStructure;
import org.kafkaApp.Structure.RequestStructure;
import org.kafkaApp.Structure.StructureWithMetaDataParameters;
import org.kafkaApp.Structure.Tuple2Dem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class RouterMicroservice {

    //configuration of Router Microservice

    private static final String MICROSERVICE_ID = "router-microservice";
    private final  static int replicateFactor = EnvironmentConfiguration.giveTheReplicationFactor();
    private static final String BOOTSTRAP_SERVERS = EnvironmentConfiguration.getBootstrapServers();//"localhost:9092,localhost:9093,localhost:9094";

    // private final KafkaStreams kafkaStreams2;
    private final KafkaStreams kafkaStreams1;
    private final static String dataTopicName1="Data_Topic";
    private final static  String reqTopicName="Request_Topic";
    private static final Logger logger = LoggerFactory.getLogger(RouterMicroservice.class);
    static AtomicInteger produceMessages = new AtomicInteger(0);
    private final LeaderElection leaderElection;


    public RouterMicroservice() {

        leaderElection = new LeaderElection();
        try {
            leaderElection.connectToZooKeeper();
            leaderElection.electLeader();
        } catch (Exception e) {
            logger.error("Failed to elect leader.", e);
        }

        //properties for ROUTER microservice

        CreateTopic createTopic = new CreateTopic();





        StreamsBuilder builder1 = new StreamsBuilder();


        CreateConfiguration createConfiguration = new CreateConfiguration();

        Properties properties= createConfiguration.getPropertiesForMicroservice(MICROSERVICE_ID, BOOTSTRAP_SERVERS);
        //properties.put(StreamsConfig.CLIENT_ID_CONFIG, "router-microservice-instance-" + UUID.randomUUID().toString());        int NewNumThreads = (int) Math.ceil((double) 3 / 2);
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, EnvironmentConfiguration.giveTheParallelDegree()/EnvironmentConfiguration.giveTheDividerForParallelDegree());
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0"); // 1MB cache size
        //properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1000"); // 1000 ms linger

        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384"); // Example: 16KB batch size
        properties.put(ProducerConfig.RETRIES_CONFIG, 3); // Example: 3 retries



        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "2048"); // Example: 1KB minimum fetch
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500"); // Example: 500 ms maximum wait
        properties.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_REQUEST_SIZE_CONFIG), "2097152"); // 2MB

        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "120000"); // Increase to 120000 ms
        //properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000"); // Increase to 120000 ms NOT
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "32108864"); // Increase buffer memory to 32MB

        //configure topics Names
        //properties.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, CustomRocksDBConfig.class.getName());
        //Set up the synopses producers
        Properties properties2 = createConfiguration.getPropertiesConfig(ListRequestStructureSerializer.class);
        Properties properties3 = createConfiguration.getPropertiesConfig(DataStructureSerializer.class);
        Properties properties4 = createConfiguration.getPropertiesConfig(StringSerializer.class);

        LoadAndSaveSynopsis loadSynopsis = new LoadAndSaveSynopsis(properties4,replicateFactor);


        final String  InstanceStatusTopicName = "InstanceStatus_Topic";
        createTopic.createMyCompactTopic(InstanceStatusTopicName, 1, replicateFactor); // create the topic which contains all the necessary infos for the instances
       /* Metrics metrics = new Metrics();
        Sensor sensor = metrics.sensor("throughputSensorRouter");
        MetricName metricName = new MetricName("throughput", "userMetrics", "Tracks throughput", new HashMap<>());
        sensor.add(metricName, new Rate());*/




        //Topology of kafka streams
        //consume data from request topic, processing, creating microservice for synopsis,write to synopsisReqTopic

        String keyForInstance= "InstanceStatus";//leaderElection.getActiveInstances().toString();
        String valueInstance = Integer.toString(leaderElection.getActiveInstanceCount());
        //properties.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,"consumer-instance-"+valueInstance);
        //properties.put("group.instance.id", "consumer-instance-"+valueInstance);
        //properties.put(StreamsConfig.CLIENT_ID_CONFIG, "custom-client-name");

        if(produceMessages.getAndIncrement()==0){
            try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties4)) {
                kafkaProducer.send(new ProducerRecord<>(InstanceStatusTopicName, keyForInstance, valueInstance));
            }
        }

        if(leaderElection.isLeader()){
            System.out.println("Create data abd Req topic");
            createTopic.createMyTopic("Data_Topic", EnvironmentConfiguration.giveTheParallelDegree(), replicateFactor);
            //Creating Request_Topic with 5 partitions and replication factor 1
            createTopic.createMyTopic("Request_Topic", EnvironmentConfiguration.giveTheParallelDegree(), replicateFactor);

            createTopic.createMyCompactTopic("OutputTopicSynopsis"+1, 1, replicateFactor); //synopsisID=1
            createTopic.createMyCompactTopic("OutputTopicSynopsis"+2, 1, replicateFactor); //synopsisID=2
            createTopic.createMyCompactTopic("OutputTopicSynopsis"+3, 1, replicateFactor); //synopsisID=3
            createTopic.createMyCompactTopic("OutputTopicSynopsis"+4, 1, replicateFactor); //synopsisID=4
           /* for(int i=1; i<=EnvironmentConfiguration.giveTheParallelDegree(); i++) {
                String intermidiateTopicName = "Intermidiate_Multy_Thread_Tpic"+"_"+i;
                createTopic.createMyCompactTopic(intermidiateTopicName, 1, 3);
            }*/

        }

        KStream<String, Tuple2Dem<List<RequestStructure>, String>> copyOfRequest = builder1.stream(reqTopicName, Consumed.with(Serdes.String(), new ListRequestStructureSerde()))
                .filter((key, value) -> !key.equals("InstanceStatus"))
                .selectKey(((key, value) -> keyForInstance))
                .join(builder1.stream(InstanceStatusTopicName, Consumed.with(Serdes.String(), Serdes.String())), Tuple2Dem::new
                        , JoinWindows.of(Duration.ofHours(1)), StreamJoined.with(Serdes.String(), new ListRequestStructureSerde(), Serdes.String()))
                // .filter(((key, value) -> leaderElection.getActiveInstanceCount()<=1))
                // .filter(((key, value) -> value!=null))
                .mapValues((key,value)->{
                    int countOfCurrentInstances = Integer.parseInt(value.getValue2());
                    if(countOfCurrentInstances<=1 || value.getValue1().get(0).getParam()[2].equals("Queryable")){//|| countOfCurrentInstances>value.getValue1().get(0).getNoOfP()){// allagh if(countOfCurrentInstances<=1 || countOfCurrentInstances>value.getValue1().get(0).getNoOfP()){// allagh
                        System.out.println("mpenei edw Sthn Sinthiki?? " +  key+","+value);
                        return null;
                    }

                    System.out.println("mpenei edw " + key+","+value);
                    return value;
                })
                .filter(((key, value) -> value!=null))
                .mapValues((key,value)->{
                    try (KafkaProducer<String, List<RequestStructure>> kafkaProducer = new KafkaProducer<>(properties2)) {
                        int whichPartitionSetUp = (Integer.parseInt(value.getValue2())-1)%EnvironmentConfiguration.giveTheParallelDegree();
                        System.out.println("whichPartitionSetUp: "+whichPartitionSetUp);
                        kafkaProducer.send(new ProducerRecord<>(reqTopicName,whichPartitionSetUp, key, value.getValue1()));
                    }
                    return value;
                });

        copyOfRequest.peek((key, value) -> System.out.println("keyForCoppy: "+key+" ,value: "+value.getValue1().get(0).getRequestID()+","+value.getValue2()));

        CustomGenericStreamPartitioner<StructureWithMetaDataParameters<RequestStructure>> partitioner = new CustomGenericStreamPartitioner<>(StructureWithMetaDataParameters::getMetaData3, 3);

        Map<String, KStream<String, RequestStructure>> consumedRequest = builder1.stream(reqTopicName, Consumed.with(Serdes.String(), new ListRequestStructureSerde()))
                .flatMap((key, list) -> {
                    List<KeyValue<String, RequestStructure>> keyValues = new ArrayList<>();

                    list.parallelStream().forEachOrdered(request -> {
                        Object[] parameters = request.getParam();
                        String field =parameters[1].toString();
                        String newKey = request.getStreamID()+","+request.getDataSetKey()+","+Integer.valueOf(request.getSynopsisID()).toString()+","+field;
                        keyValues.add(new KeyValue<>(newKey, request));
                    });
                    return keyValues;
                })
                .split(Named.as("Request-"))
                .branch((key, value) -> value.getParam()[0].equals("LOAD_REQUEST"),Branched.as("LOAD"))
                .branch((key, value) -> !value.getParam()[0].equals("LOAD_REQUEST"),Branched.as("Normal"))
                .defaultBranch();

        consumedRequest.get("Request-Normal").peek((key, value) -> System.out.println("keyRouterReq: "+key+" ,value: "+value.getRequestID()+","+value.getParam()[2]));
        KTable<String, StructureWithMetaDataParameters<RequestStructure>> createdMicroservicesTable = loadSynopsis.loadRequestedSynopsis(consumedRequest.get("Request-LOAD"))
                .merge(consumedRequest.get("Request-Normal"))
                .groupByKey(Grouped.with(Serdes.String(), new RequestStructureSerde()))
                .aggregate(() -> new StructureWithMetaDataParameters<>(null, 0, 0,0,false), (key, value, aggregate) ->{
                            aggregate.setMetaData1(aggregate.getMetaData1()+1); //group count
                            //here check if GKquantilie to setup to noOfp=1
                            if(value.getParam()[2].equals("NotQueryable")){
                                aggregate.setMetaData3(aggregate.getMetaData3()+1);
                                if(aggregate.getMetaData1()<=1 || value.getParam()[0].equals("LOAD_REQUEST") ){
                                    try {
                                        int topicCounter =leaderElection.incrementAndGet();
                                       // int topicCounter =value.getRequestID();
                                        aggregate.setMetaData2(topicCounter);//topic count change with state store because didnt work in distibuted processing
                                        String reqTopicName = "ReqTopicSynopsis"+"_"+topicCounter;
                                        String dataTopicName = "DataTopicSynopsis"+"_"+topicCounter;

                                        createTopic.createMyTopic(dataTopicName, value.getNoOfP(), replicateFactor);
                                        String intermidiateTopicName = "Intermidiate_Multy_Thread_Tpic"+"_"+topicCounter;
                                        createTopic.createMyCompactTopic(intermidiateTopicName, 1, replicateFactor);
                                        createTopic.createMyTopic(reqTopicName, value.getNoOfP(), replicateFactor);


                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }

                                }
                            }
                            return new StructureWithMetaDataParameters<>(value, aggregate.getMetaData1(),aggregate.getMetaData2(),aggregate.getMetaData3(),false);
                        } , Materialized.<String, StructureWithMetaDataParameters<RequestStructure>, KeyValueStore<Bytes, byte[]>>as("ManageTopicCounter")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new RequestWithMetaDataParametersSerde())
                        //Materialized.with(Serdes.String(), new RequestWithMetaDataParametersSerde())
                )
                .toStream()
                .repartition(Repartitioned.<String, StructureWithMetaDataParameters<RequestStructure>>as("SpreadRequestIntoAllPartitions")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(new RequestWithMetaDataParametersSerde())
                        .withStreamPartitioner(partitioner))
                .mapValues((key,value)->{
                    RequestStructure request= value.getReq();
                    int synopsisID = request.getSynopsisID();
                    int topicCount = value.getMetaData2();

                    System.out.println("topicCountNum: "+topicCount);
                    String reqTopicName = "ReqTopicSynopsis"+"_"+topicCount;


                    int numPartitions = value.getReq().getNoOfP();
                   // int numOfTotalInstances = leaderElection.getActiveInstanceCount();
                   // int numOfThreads=(int) Math.ceil((double) numPartitions /numOfTotalInstances);

                    System.out.println("getMetaData3: "+value.getMetaData3());
                    if(value.getMetaData3()<=1 || value.getReq().getParam()[2].equals("Queryable")) {//prosorino veltiosi

                            try (KafkaProducer<String, RequestStructure> kafkaProducer = new KafkaProducer<>(properties2)) {
                                IntStream.range(0, numPartitions).forEach(i -> {
                                    request.setPartition(i);
                                    kafkaProducer.send(new ProducerRecord<>(reqTopicName, i, key, request));
                                });
                            }

                    }
                    //thelei vetiosi
                    if (value.getReq().getParam()[2].equals("Queryable") && !value.getReq().getParam()[0].equals("LOAD_REQUEST")) {
                        System.out.println("Microservice already created");
                        return value;
                    }
                    System.out.println("Creating microservice ");
                    if(synopsisID==0){
                        RawDataMicroService rawDataMicroService = new RawDataMicroService(topicCount,numPartitions);
                        rawDataMicroService.clear();
                        rawDataMicroService.start();

                    }

                    if (synopsisID == 1) {//COUNTMIN
                        value.setMergedSynopses(true);
                        GenericSynopsesMicroService synopsesMicroService1 = new GenericSynopsesMicroService(topicCount,numPartitions,request.getSynopsisID(),"CountMin",key,true);
                        synopsesMicroService1.clear();
                        synopsesMicroService1.start();

                    } else if(synopsisID == 2){//HYPERLOGLOG
                        value.setMergedSynopses(true);
                        GenericSynopsesMicroService synopsesMicroService2= new GenericSynopsesMicroService(topicCount,numPartitions,request.getSynopsisID(),"HyperLogLog",key,true);
                        synopsesMicroService2.clear();
                        synopsesMicroService2.start();
                    }
                    else if(synopsisID == 3 ){//BLOOMFILTER
                        value.setMergedSynopses(true);
                        GenericSynopsesMicroService synopsesMicroService3= new GenericSynopsesMicroService(topicCount,numPartitions,request.getSynopsisID(),"BloomFilter",key,true);
                        synopsesMicroService3.clear();
                        synopsesMicroService3.start();
                    }
                    else if(synopsisID == 4 ){//DFT
                        value.setMergedSynopses(false);
                        GenericSynopsesMicroService synopsesMicroService4= new GenericSynopsesMicroService(topicCount,numPartitions,request.getSynopsisID(),"DFT",key,false);
                        synopsesMicroService4.clear();
                        synopsesMicroService4.start();
                    }
                    else if(synopsisID == 5 ){//LossyCounting
                        value.setMergedSynopses(false);
                        GenericSynopsesMicroService synopsesMicroService5= new GenericSynopsesMicroService(topicCount,numPartitions,request.getSynopsisID(),"LossyCounting",key,false);
                        synopsesMicroService5.clear();
                        synopsesMicroService5.start();
                    }else if(synopsisID == 6 ){//StickySampling
                        value.setMergedSynopses(false);
                        GenericSynopsesMicroService synopsesMicroService6= new GenericSynopsesMicroService(topicCount,numPartitions,request.getSynopsisID(),"StickySampling",key,false);
                        synopsesMicroService6.clear();
                        synopsesMicroService6.start();
                    }
                    else if(synopsisID == 7 ){//AMSSketch
                        value.setMergedSynopses(false);
                        GenericSynopsesMicroService synopsesMicroService7= new GenericSynopsesMicroService(topicCount,numPartitions,request.getSynopsisID(),"AMSSketch",key,false);
                        synopsesMicroService7.clear();
                        synopsesMicroService7.start();
                    }
                    else if(synopsisID == 8 ){//GKQuantiles
                        value.setMergedSynopses(false);
                        GenericSynopsesMicroService synopsesMicroService8= new GenericSynopsesMicroService(topicCount,numPartitions,request.getSynopsisID(),"GKQuantiles",key,false);
                        synopsesMicroService8.clear();
                        synopsesMicroService8.start();
                    }
                    else if(synopsisID == 9 ){//LSH
                        value.setMergedSynopses(false);
                        GenericSynopsesMicroService synopsesMicroService9= new GenericSynopsesMicroService(topicCount,numPartitions,request.getSynopsisID(),"LSH",key,false);
                        synopsesMicroService9.clear();
                        synopsesMicroService9.start();
                    }
                    else if(synopsisID == 10 ){//WindowSketchQuantilesSynopsis
                        value.setMergedSynopses(false);
                        GenericSynopsesMicroService synopsesMicroService10= new GenericSynopsesMicroService(topicCount,numPartitions,request.getSynopsisID(),"WindowSketchQuantiles",key,false);
                        synopsesMicroService10.clear();
                        synopsesMicroService10.start();
                    }

                    return value;
                }).toTable();




        Map<String,KStream<String, StructureWithMetaDataParameters<RequestStructure>>> branchedBasedOnStreamID =createdMicroservicesTable.toStream()
                .split(Named.as("StreamID-"))
                .branch((key, value) -> value.getReq().getStreamID().isEmpty() && ( value.getMetaData1() == 1 && value.getReq().getParam()[2].equals("NotQueryable")  ),Branched.as("Empty"))
                .branch((key, value) -> !value.getReq().getStreamID().isEmpty() && ( value.getMetaData1() == 1 && value.getReq().getParam()[2].equals("NotQueryable")  ),Branched.as("NoEmpty"))
                .defaultBranch();


        CustomGenericStreamPartitioner<StructureWithMetaDataParameters<RequestStructure>> partitioner2 = new CustomGenericStreamPartitioner<>(StructureWithMetaDataParameters::getMetaData1, EnvironmentConfiguration.giveTheParallelDegree());

        //here we do flatmap to create 3 requests as the number of portions in Data topic to join the data in parallel processing
        KStream<String, StructureWithMetaDataParameters<RequestStructure>> emptyStreamIdReq = branchedBasedOnStreamID.get("StreamID-Empty")
                .selectKey((key, value) -> value.getReq().getDataSetKey())
                .flatMap((key, value) -> {
                    List<KeyValue<String, StructureWithMetaDataParameters<RequestStructure>>> keyValues = new ArrayList<>();
                    System.out.println("key122: " + key);
                    for (int i = 0; i < EnvironmentConfiguration.giveTheParallelDegree(); i++) {
                        String newKey =  value.getReq().getDataSetKey();
                        StructureWithMetaDataParameters<RequestStructure> newValue = new StructureWithMetaDataParameters<>(value.getReq(), i, value.getMetaData2(), value.getMetaData3(), value.isMergedSynopses());
                        keyValues.add(new KeyValue<>(newKey, newValue));
                    }
                    return keyValues;
                })
                .repartition(Repartitioned.<String, StructureWithMetaDataParameters<RequestStructure>>as("joinedReqEmptyRepartitionedRouter")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(new RequestWithMetaDataParametersSerde())
                        .withStreamPartitioner(partitioner2));


        KStream<String, StructureWithMetaDataParameters<RequestStructure>> noEmptyStreamIdReq = branchedBasedOnStreamID.get("StreamID-NoEmpty")
                .selectKey((key, value) -> value.getReq().getStreamID()+","+value.getReq().getDataSetKey())
                .flatMap((key, value) -> {
                    List<KeyValue<String, StructureWithMetaDataParameters<RequestStructure>>> keyValues = new ArrayList<>();
                    System.out.println("key122: " + key);
                    for (int i = 0; i < EnvironmentConfiguration.giveTheParallelDegree(); i++) {
                        String newKey = value.getReq().getStreamID() + "," + value.getReq().getDataSetKey();
                        StructureWithMetaDataParameters<RequestStructure> newValue = new StructureWithMetaDataParameters<>(value.getReq(), i, value.getMetaData2(), value.getMetaData3(), value.isMergedSynopses());
                        keyValues.add(new KeyValue<>(newKey, newValue));
                    }
                    return keyValues;
                })
                .repartition(Repartitioned.<String, StructureWithMetaDataParameters<RequestStructure>>as("joinedReqNoEmptyRepartitionedRouter")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(new RequestWithMetaDataParametersSerde())
                        .withStreamPartitioner(partitioner2));

        Map<String, KStream<String, StructureWithMetaDataParameters<RequestStructure>>> branchedBasedMergedSynopses =emptyStreamIdReq
                .split(Named.as("Merged-"))
                .branch((key, value) ->  !value.isMergedSynopses(),Branched.as("NO"))
                .branch((key, value) ->  value.isMergedSynopses(),Branched.as("YES"))
                .defaultBranch();
        branchedBasedMergedSynopses.get("Merged-NO").foreach((key, value) -> System.out.println("how merges Req we have ,"+value.getReq().getRequestID()+","+Thread.currentThread().getName()));

        //HERE BEGIN DATA LOGIC
        KStream<String, DataStructure> readDataTopic = builder1.stream(dataTopicName1, Consumed.with(Serdes.String(), new DataStructureSerde()));
        //readDataTopic.transformValues(ThroughputSensor.createThroughputSensor("ingestionRouter"));



        //readDataTopic.transformValues(TotalProcessedDataSensor.<List<DataStructure>>createProcessedDataSensor("ingestionRouter"));

      /*  readDataTopic.foreach((key, value) -> {
            System.out.println("keyDataTopic: "+key);
            System.out.println("valueDataTopic: "+value.size());
        });*/





        KStream<String, Tuple2Dem<DataStructure,StructureWithMetaDataParameters<RequestStructure>>> combinedStreamAllDataset = readDataTopic
                .selectKey((key, value) -> value.getDataSetKey())
                .repartition(Repartitioned.<String, DataStructure>as("DataRoundRobinRepartitionedRouter")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(new DataStructureSerde())
                        .withStreamPartitioner(new CustomStreamPartitioner4()))
                .join(branchedBasedMergedSynopses.get("Merged-YES"), Tuple2Dem::new, JoinWindows.of(Duration.ofHours(1)),
                        StreamJoined.with(Serdes.String(), new DataStructureSerde(),new RequestWithMetaDataParametersSerde()));
        //combinedStream.transformValues(ThroughputSensor.createThroughputSensor("joinedRouter"));


        /*KStream<String, Tuple2Dem<DataStructure,StructureWithMetaDataParameters<RequestStructure>>> combinedStreamForNoMergableSynopsesAllDataset = readDataTopic
                .selectKey((key, value) -> value.getDataSetKey())
                .repartition(Repartitioned.<String, DataStructure>as("DataStreamIDRepartitionedRouter")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(new DataStructureSerde())
                        .withStreamPartitioner(new CustomStreamPartitioner3()))
                .join(branchedBasedMergedSynopses.get("Merged-NO"), Tuple2Dem::new, JoinWindows.of(Duration.ofHours(1)),
                        StreamJoined.with(Serdes.String(), new DataStructureSerde(),new RequestWithMetaDataParametersSerde()));*/


       /* combinedStreamForNoMergableSynopses.foreach((key, value) -> {
            System.out.println("iS an empty how much joins had made ,"+value.getValue2().getReq().getRequestID()+","+Thread.currentThread().getName());
        });*/


        KStream<String, Tuple2Dem<DataStructure,StructureWithMetaDataParameters<RequestStructure>>> combinedStreamSpesificStreamID = readDataTopic
                .repartition(Repartitioned.<String, DataStructure>as("DataRoundRobinRepartitionedRouter2")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(new DataStructureSerde())
                        .withStreamPartitioner(new CustomStreamPartitioner4()))
                .join(noEmptyStreamIdReq, Tuple2Dem::new, JoinWindows.of(Duration.ofHours(1)),
                        StreamJoined.with(Serdes.String(), new DataStructureSerde(),new RequestWithMetaDataParametersSerde()));



       /* combinedStream2.foreach((key, value) -> {
            System.out.println("NO STREAM empty how much joins had made,"+value.getValue2().getReq().getRequestID()+","+Thread.currentThread().getName());
        });*/

        KafkaProducer<String, DataStructure> kafkaProducer2 = new KafkaProducer<>(properties3);
        AtomicInteger nextPartitionIndex = new AtomicInteger(0);
        Map<String, Integer> partionCountManager =  new ConcurrentHashMap<>();


       /* combinedStreamForNoMergableSynopsesAllDataset.foreach((key, value) -> {
            if(partionCountManager.get(value.getValue1().getStreamID())==null){
                partionCountManager.put(value.getValue1().getStreamID(),nextPartitionIndex.getAndIncrement());
            }

            int partitionIndex = partionCountManager.get(value.getValue1().getStreamID());
            //System.out.println("data get inside here?"+value.getValue2().getMetaData3());
            String dataTopicName = "DataTopicSynopsis"+"_"+value.getValue2().getMetaData2(); //Topic Number

            kafkaProducer2.send(new ProducerRecord<>(dataTopicName,partitionIndex ,","+key , value.getValue1())); //value1 is the data
            // You might want to do this at some logical points in your code or on a schedule.
            //kafkaProducer2.flush();

            //kafkaProducer2.close();
        });*/

        Map<String, AtomicInteger> topicPartitionCounters = new ConcurrentHashMap<>();
        // Map<String, AtomicInteger> topicPartitionCounters = new HashMap<>();

        combinedStreamAllDataset.merge(combinedStreamSpesificStreamID)
                //.transformValues(new ProcessedDataTransformSupplier(),"ProcessDataStore")
                //.filter(((key, value) -> value!=null))
                .foreach((key, value) -> {
                    //System.out.println("data is proc?,"+value.getIsProcessed());
                    String dataTopicName = "DataTopicSynopsis"+"_"+value.getValue2().getMetaData2(); //Topic Number
                    AtomicInteger partitionCounter = topicPartitionCounters.computeIfAbsent(dataTopicName, k -> new AtomicInteger(0));
                    int partition = partitionCounter.getAndIncrement() % value.getValue2().getReq().getNoOfP();

                    /*nextPartitionIndex2.computeIfAbsent(key, k -> new AtomicInteger(0)).incrementAndGet(); // Increment count for the thread
                    if(nextPartitionIndex2.get(key).get()%10000==0||nextPartitionIndex2.get(key).get()==132197 || nextPartitionIndex2.get(key).get()==151105)
                        System.out.println( " totalDataSend for key " + key + ": " + nextPartitionIndex2.get(key));*/

                    //kafkaProducer2.send(new ProducerRecord<>(dataTopicName, partition, key, value.getValue1()));
                    String newKey=key;
                    if(value.getValue2().getReq().getStreamID().isEmpty()){
                        newKey= ","+key;
                    }

                    kafkaProducer2.send(new ProducerRecord<>(dataTopicName, partition, newKey, value.getValue1()));
                    // You might want to do this at some logical points in your code or on a schedule.
                    //kafkaProducer2.flush();

                    //kafkaProducer2.close();
                });
       /* combinedStream.merge(combinedStream2).mapValues((key, value) -> {
            return value.getValue1();
        })
                .to((key, value, recordContext) -> {
            return "DataTopicSynopsis" + "_" + value.getValue2().getMetaData2();
        }, Produced.with(Serdes.String(), new DataStructureSerde()));*/

       /* KStream<String,StructureWithMetaDataParameters<DataStructure>> processedStream = combinedStream.merge(combinedStream2)
                .mapValues((key, value) -> {
                    String dataTopicName = "DataTopicSynopsis" + "_" + value.getValue2().getMetaData2();
                    AtomicInteger partitionCounter = topicPartitionCounters.computeIfAbsent(dataTopicName, k -> new AtomicInteger(0));
                    int partition = partitionCounter.getAndIncrement() % value.getValue2().getReq().getNoOfP();

                    return new StructureWithMetaDataParameters<>(value.getValue1(), value.getValue2().getMetaData2(),partition,0,false);
                });

        processedStream.to(new TopicNameExtractor<String, KeyValue<String,StructureWithMetaDataParameters<DataStructure>>>() {
            @Override
            public String extract(String key, KeyValue<String, StructureWithMetaDataParameters<DataStructure>> value, RecordContext recordContext) {
                return "DataTopicSynopsis" + "_" + value.value.getMetaData1();
            }

        }.toString(), Produced.with(Serdes.String(), new DataWithMetaDataParametersSerde()));*/

// Send data to the computed topic and partition
        //combinedStream.to((key, value, recordContext) -> value.getValue1(), Produced.with(Serdes.String(), new DataStructureSerde()));
       // System.out.println("topology result "+builder1.build().describe());
        this.kafkaStreams1 = new KafkaStreams(builder1.build(), properties);

    }

    public void start() {
        kafkaStreams1.start();

    }
    public void stop() {

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                kafkaStreams1.close(Duration.ofSeconds(5));  // Wait up to 5 seconds for all processing to complete
                clear();
            }
        });
        try {
            leaderElection.close();
        } catch (InterruptedException e) {
            logger.error("Failed to close ZooKeeper connection.", e);
        }
    }
    public void clear() {
        kafkaStreams1.cleanUp();
    }




        /*
        StoreBuilder<KeyValueStore<String, Long>> storeTopicCounterStateStore =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("topic-counter-state-store"),
                        Serdes.String(),
                        Serdes.Long()
                ).withLoggingDisabled();  // Logging is generally disabled for global stores.

        builder.addGlobalStore(
                storeTopicCounterStateStore,
                TopicCounterTopicName,  // Specify the name of the source topic here.
                Consumed.with(Serdes.String(), Serdes.Long()),
                new ProcessorSupplier<String, Long>() {
                    @Override
                    public Processor<String, Long> get() {
                        return new Processor<String, Long>() {
                            private KeyValueStore<String, Long> stateStore;
                            private static final String Topic_StateStore_COUNTER_KEY = "topic-count";

                            @Override
                            public void init(ProcessorContext context) {
                                this.stateStore = context.getStateStore("topic-counter-state-store");
                            }

                            @Override
                            public void process(String key, Long value) {
                                Long currentCount = stateStore.get(Topic_StateStore_COUNTER_KEY);
                                System.out.println("1currentCount: "+currentCount);
                                if (currentCount == null) {
                                    currentCount = 0L;
                                }
                                currentCount +=value;  // increment by the value
                                System.out.println("2currentCount: "+currentCount);
                                stateStore.put(Topic_StateStore_COUNTER_KEY, currentCount);
                                stateStore.put(key, currentCount);
                                //return currentCount;
                            }
                            @Override
                            public void close() {

                            }


                        };
                    }
                }
        );


         */





    public static void main(String[] args) {
        //init microservice

        RouterMicroservice routerMicroservice = new RouterMicroservice();
        routerMicroservice.clear();
        routerMicroservice.start();// start streams

        // Get metrics

        //routerMicroservice.stop();

    }
}