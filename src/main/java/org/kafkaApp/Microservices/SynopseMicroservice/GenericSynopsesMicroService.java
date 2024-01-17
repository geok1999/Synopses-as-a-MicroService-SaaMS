package org.kafkaApp.Microservices.SynopseMicroservice;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.kafkaApp.Configuration.CreateConfiguration;
import org.kafkaApp.Configuration.EnvironmentConfiguration;
import org.kafkaApp.Microservices.Router.LoadAndSaveSynopsis;
import org.kafkaApp.Microservices.Transformers.*;
import org.kafkaApp.Partitioners.CustomGenericStreamPartitioner;
import org.kafkaApp.Serdes.Init.DataStructure.DataStructureSerde;
import org.kafkaApp.Serdes.Init.EstimationResult.EstimationResultSerde;
import org.kafkaApp.Serdes.Init.RequestStructure.RequestStructureSerde;
import org.kafkaApp.Serdes.Init.Tuple2Dem.KTableMergejoinTuple2DemSerde;
import org.kafkaApp.Serdes.Init.Tuple2Dem.LoadTupleTuple2demSerde;
import org.kafkaApp.Serdes.SynopsesSerdes.SynopsesAndRequest.SynopsisAndRequestSerde;
import org.kafkaApp.Serdes.SynopsesSerdes.SynopsesSerdes;
import org.kafkaApp.Serdes.SynopsesSerdes.SynopsisAndParameters.SynopsisAndParametersSerde;
import org.kafkaApp.Structure.*;
import org.kafkaApp.Synopses.Synopsis;
import org.streaminer.stream.frequency.util.CountEntry;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class GenericSynopsesMicroService {
    private KafkaStreams kafkaStreams;

    private static final String BOOTSTRAP_SERVERS = EnvironmentConfiguration.getBootstrapServers();//"localhost:9092,localhost:9093,localhost:9094";
    protected final String SYNOPSES_STORE_NAME;
    protected final String BatchStateStore_STORE_NAME;
    protected final String LOAD_TOPIC_NAME;
    protected final int TOPIC_COUNT;

    private AtomicInteger recordsProcessed = new AtomicInteger(0);

    /*private static Properties properties = new Properties();

    static {
        try (InputStream input = new FileInputStream("/home/gkalfakis/Configuration/config.properties")) {
            properties.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
            // Handle exception, e.g., by falling back to default values or terminating the application.
        }
    }*/

    public GenericSynopsesMicroService(int topicCount, int numThreads, int SynopsisId, String synopsesType,String keyBuild,boolean mergedSynopsesControrler) {

        final String MICROSERVICE_ID = "synopsis"+topicCount+"microservice";
        final String reqTopicName = "ReqTopicSynopsis"+"_"+topicCount;
        final String dataTopicName = "DataTopicSynopsis"+"_"+topicCount;
        final String intermidiateTopicName = "Intermidiate_Multy_Thread_Tpic"+"_"+topicCount;


        this.SYNOPSES_STORE_NAME = synopsesType+"Store";
        this.BatchStateStore_STORE_NAME = "StateStorebatchTable"+topicCount;
        this.LOAD_TOPIC_NAME = "Load"+synopsesType+"Synopses_Topic";
        this.TOPIC_COUNT= topicCount;

        //build and configure synopsis consumer
        StreamsBuilder builder = new StreamsBuilder();

        CreateConfiguration createConfiguration=new CreateConfiguration();

        Properties properties = createConfiguration.getPropertiesForMicroservice(MICROSERVICE_ID, BOOTSTRAP_SERVERS);

        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numThreads/EnvironmentConfiguration.giveTheDividerForParallelDegree());
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "248576"); //1048576    1MB cache size
        //properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

       // properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "548576"); //1048576    1MB cache size
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "4384"); //16384 Example: 16KB batch size
        properties.put(ProducerConfig.RETRIES_CONFIG, 3); // Example: 3 retries


        //properties.put(ProducerConfig.LINGER_MS_CONFIG, "100"); // Example: 100 ms linger
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "548"); //2048 Example: 1KB minimum fetch
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "100"); //500 Example: 500 ms maximum wait
        properties.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_REQUEST_SIZE_CONFIG), "1097152"); //4097152 2MB

        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "120000"); // Increase to 120000 ms
       // properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000"); // Increase to 120000 ms
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "16108864"); // Increase buffer memory to 32MB
        //properties.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, CustomRocksDBConfig.class.getName());



       /* properties = createConfiguration.getPropertiesForMicroservice(MICROSERVICE_ID, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numThreads/EnvironmentConfiguration.giveTheDividerForParallelDegree());
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, properties.getProperty("streams.cache.max.bytes.buffering"));
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, properties.getProperty("producer.batch.size"));
        properties.put(ProducerConfig.RETRIES_CONFIG, Integer.parseInt(properties.getProperty("producer.retries")));
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, properties.getProperty("producer.compression.type"));
        properties.put(ProducerConfig.ACKS_CONFIG, properties.getProperty("producer.acks"));
        properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, properties.getProperty("consumer.fetch.min.bytes"));
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, properties.getProperty("consumer.fetch.max.wait.ms"));
        properties.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_REQUEST_SIZE_CONFIG), properties.getProperty("producer.max.request.size"));
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, properties.getProperty("producer.max.block.ms"));
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, properties.getProperty("producer.delivery.timeout.ms"));
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, properties.getProperty("producer.buffer.memory"));
*/

        StoreBuilder<KeyValueStore<String, Synopsis>> synopsesStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(SYNOPSES_STORE_NAME),
                        Serdes.String(),
                        new SynopsesSerdes()
                );


        StoreBuilder<KeyValueStore<String, SynopsisAndParameters>> synopsesTableStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("SynopsisTable-State-Store"),
                        Serdes.String(),
                        new SynopsisAndParametersSerde()
                );

        StoreBuilder<KeyValueStore<String, EstimationResult>> estimateSynopsesStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("ESTIMATE_SYNOPSES_STORE_NAME"),
                        Serdes.String(),
                        new EstimationResultSerde()
                );

        builder.build().addStateStore(synopsesStoreBuilder);
        builder.build().addStateStore(synopsesTableStoreBuilder);
        builder.build().addStateStore(estimateSynopsesStoreBuilder);
        KTable<String, Synopsis> loadedSynopsisInst = builder.table(this.LOAD_TOPIC_NAME, Consumed.with(Serdes.String(), new SynopsesSerdes()));

        Map<String, KStream<String, RequestStructure>> processReqBranch = builder.stream(reqTopicName, Consumed.with(Serdes.String(), new RequestStructureSerde()))
                .split(Named.as("Request-"))
                .branch((key, value) -> value.getParam()[2].equals("NotQueryable"),Branched.as("Structure"))
                .branch((key, value) -> value.getParam()[2].equals("Queryable"),Branched.as("Query"))
                .defaultBranch();

        CustomGenericStreamPartitioner<SynopsisAndRequest> partitioner = new CustomGenericStreamPartitioner<>(synReq -> synReq.getRequest().getPartition(), numThreads);

        KTable<String, SynopsisAndRequest> structureSynopses = processReqBranch.get("Request-Structure")
                .transform(new SynopsisTransformerSupplier(SYNOPSES_STORE_NAME), SYNOPSES_STORE_NAME)
                .repartition(Repartitioned.<String, SynopsisAndRequest>as("structureReqRepartitioned")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(new SynopsisAndRequestSerde())
                        .withStreamPartitioner(partitioner))
                .toTable();


        //AtomicInteger counter2 = new AtomicInteger(0);
        KStream<String,SynopsisAndParameters> batchProcessing = builder.stream(dataTopicName, Consumed.with(Serdes.String(), new DataStructureSerde()))
                .transform(() -> new AddBatchDataToSynopsesStateStoreTransformer(Duration.ofSeconds(Long.parseLong(EnvironmentConfiguration.TimeWaitToDoAdd())),SYNOPSES_STORE_NAME),SYNOPSES_STORE_NAME);

         batchProcessing.filterNot((key, value) ->(Integer)value.getParameters()[2]<=1) //filter the single thread synopsis
             .to(intermidiateTopicName,Produced.with(Serdes.String(), new SynopsisAndParametersSerde()));



        CustomGenericStreamPartitioner<RequestStructure> partitioner2 = new CustomGenericStreamPartitioner<>(RequestStructure::getPartition, numThreads);

        KStream<String,RequestStructure> queryReqStream =processReqBranch.get("Request-Query").selectKey((key, value) ->{
                    if(value.getStreamID().isEmpty()) {
                        return ","+value.getDataSetKey()+","+value.getRequestID();
                    }
                    return value.getStreamID()+","+value.getDataSetKey();
                })
                .repartition(Repartitioned.<String,RequestStructure>as("joinedReqRepartitioned2")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(new RequestStructureSerde())
                        .withStreamPartitioner(partitioner2));



        KStream<String, String> singleThreadSynopsesResult = queryReqStream
                .join(batchProcessing.filterNot((key, value) ->(Integer)value.getParameters()[2]>1).selectKey((key, value) ->value.getParameters()[5]+","+value.getParameters()[6]), Tuple2Dem::new, JoinWindows.of(Duration.ofHours(2)),
                        StreamJoined.with(Serdes.String(), new RequestStructureSerde(), new SynopsisAndParametersSerde()))
                .transformValues(new SingleThreadTransformSupplier(SYNOPSES_STORE_NAME,TOPIC_COUNT),SYNOPSES_STORE_NAME)
                .filter((key, value) -> value != null);


        KStream<String, SynopsisAndParameters> subSynopses = builder.stream(intermidiateTopicName, Consumed.with(Serdes.String(), new SynopsisAndParametersSerde()));
               // .transform(()->new TotalOutputTopicTransformer("finalSynopses-byte-count",synopsesType));

        KTable<String,RequestStructure> queryTableMeged = queryReqStream
                .toTable(Materialized.with(Serdes.String(),new RequestStructureSerde()));

       // merged Synopsis and DFT
        KTable<String, SynopsisAndParameters> mergedSynopses = subSynopses.filter((key, value) -> mergedSynopsesControrler)
                .transform(() -> new WindowAggregatorForMergeTransformer(Duration.ofSeconds(20),"SynopsisTable-State-Store",numThreads,keyBuild,synopsesType),"SynopsisTable-State-Store")
                .merge(subSynopses.filter((key,value)->value.getSynopsis().getSynopsesID()==4 || value.getSynopsis().getSynopsesID()==9))
                .selectKey((key, value) -> {
                    String[] splitKey = key.split(",");
                    return splitKey[0] + "," + splitKey[1];
                }).toTable(Materialized.with(Serdes.String(),new SynopsisAndParametersSerde()));


        KTable<String, SynopsisAndParameters> totalSynopsisTable  = mergedSynopses.outerJoin(loadedSynopsisInst, Tuple2Dem::new)
                .mapValues((key, value) -> {
                        if (value.getValue1() == null) {
                          // Build a synopsis and parameter value to estiamte for load synopsis
                            String[] synopsesParameters = value.getValue2().getSynopsisDetails().split(",");
                            // Preparing parameters for SynopsisAndParameters
                            Object[] parameters = new Object[7];
                            parameters[0] = synopsesParameters[0]; // requestID
                            parameters[1] = synopsesParameters[1]; // partitionID
                            parameters[2] = Integer.parseInt(synopsesParameters[2]); // numberOfProcId
                            parameters[3] = synopsesParameters[3]; // field
                            parameters[4] = synopsesParameters[4]; // uid
                            parameters[5] = synopsesParameters[5]; // streamID
                            parameters[6] = synopsesParameters[6]; // DataSetKey
                            return new SynopsisAndParameters(value.getValue2(),parameters,0);

                        } else if (value.getValue2() == null) {
                            // return the current synopsis and parameter value to estimate original synopsis
                            return value.getValue1();
                        } else {
                            // do a merge and return the merged synopsis and parameter to estimate the total synopsis
                            Synopsis synopsisInst = null;
                            synopsisInst =   value.getValue1().getSynopsis().merge(value.getValue2());
                            synopsisInst.setSynopsesID(value.getValue2().getSynopsesID());
                            synopsisInst.setSynopsisDetails(value.getValue2().getSynopsisDetails());
                            synopsisInst.setSynopsisParameters(value.getValue2().getSynopsisParameters());
                            return new SynopsisAndParameters(synopsisInst,value.getValue1().getParameters(),0);
                        }
                },Materialized.with(Serdes.String(), new SynopsisAndParametersSerde()));

        KTable<String, Tuple2Dem<RequestStructure, SynopsisAndParameters>> joinedTableMergableSynopsis =queryTableMeged .join(
                totalSynopsisTable,leftValue -> leftValue.getStreamID()+","+leftValue.getDataSetKey(),
                Tuple2Dem::new
        );

       /* KTable<String, Tuple2Dem<RequestStructure, SynopsisAndParameters>> joinedTableMergableSynopsis =queryTableMeged .join(
                mergedSynopses,leftValue -> leftValue.getStreamID()+","+leftValue.getDataSetKey(),
                Tuple2Dem::new,Materialized.with(Serdes.String(), new KTableMergejoinTuple2DemSerde())
        );

        joinedTableMergableSynopsis.toStream().peek((key,value)->System.out.println("before Loaded Synopsis"+key));

        /* KTable<String,Tuple2Dem<Tuple2Dem<RequestStructure, SynopsisAndParameters>,Synopsis>> joinedTableMergableWithLoadSynopsis = joinedTableMergableSynopsis
                .join(loadedSynopsisInst, Tuple2Dem::new,Materialized.with(Serdes.String(),new LoadTupleTuple2demSerde()));

        joinedTableMergableWithLoadSynopsis.toStream().peek((key,value)->System.out.println("Loaded Synopsis"+key));
       /* KTable<String,String> estimationMergedSynopsis =  joinedTableMergableWithLoadSynopsis.mapValues((key,value)->{
                    Object data = value.getValue1().getValue1().getParam()[0];
                    Synopsis synopsisInst = null;
                    if(value.getValue2()==null)
                        synopsisInst = value.getValue1().getValue2().getSynopsis();
                    else if(value.getValue1()==null){
                        System.out.println("--------------------------------------------------------------------------------------");
                        System.out.println(defineEstimationSynopsis(value.getValue1().getValue1(), value.getValue2(), data));
                        System.out.println("--------------------------------------------------------------------------------------");

                    }
                    else {
                        synopsisInst =   value.getValue1().getValue2().getSynopsis().merge(value.getValue2());
                    }


                     int requestId = value.getValue1().getValue1().getRequestID(); // Assuming you have a method to get request ID
                    AtomicLong adhocCounter = adhocCounters.computeIfAbsent(requestId, k -> new AtomicLong(0));

                    if(adhocCounter.get()>1 && value.getValue1().getValue1().getParam()[3].equals("Ad-hoc")){
                        return null;
                    }
                    adhocCounter.incrementAndGet();
                    System.out.println("adhoc:"+adhocCounter.get());
                    String fieldParameter = (String) value.getValue1().getValue1().getParam()[1];
                    LoadAndSaveSynopsis loadAndSaveSynopsis = new LoadAndSaveSynopsis();


                    String synopsesMessage=null;



                    synopsesMessage = defineEstimationSynopsis(value.getValue1().getValue1(), synopsisInst, data);

                    System.out.println("--------------------------------------------------------------------------------------");
                    System.out.println(synopsesMessage);
                    System.out.println("--------------------------------------------------------------------------------------");
                    return synopsesMessage;
                })
                .filter((key, value) -> value != null);

         */

        ConcurrentHashMap<Integer, AtomicLong> adhocCounters = new ConcurrentHashMap<>();
        KTable<String,String> estimationMergedSynopsis = joinedTableMergableSynopsis
               // .leftJoin(loadedSynopsisInst, Tuple2Dem::new)
                .mapValues((key,value)->{
                    Synopsis synopsisInst = value.getValue2().getSynopsis();
                     int requestId = value.getValue1().getRequestID(); // Assuming you have a method to get request ID
                    AtomicLong adhocCounter = adhocCounters.computeIfAbsent(requestId, k -> new AtomicLong(0));

                    if(adhocCounter.get()>1 && value.getValue1().getParam()[3].equals("Ad-hoc")){
                        return null;
                    }
                    adhocCounter.incrementAndGet();
                    System.out.println("adhoc:"+adhocCounter.get());
                    String fieldParameter = (String) value.getValue1().getParam()[1];
                    LoadAndSaveSynopsis loadAndSaveSynopsis = new LoadAndSaveSynopsis();


                    String synopsesMessage=null;

                    Object data = value.getValue1().getParam()[0];

                    synopsesMessage = defineEstimationSynopsis(value.getValue1(), loadAndSaveSynopsis, synopsisInst, topicCount, data);

                    System.out.println("--------------------------------------------------------------------------------------");
                    System.out.println(synopsesMessage);
                    System.out.println("--------------------------------------------------------------------------------------");
                    return synopsesMessage;
                })
                .filter((key, value) -> value != null);


        // no merged synopsis here
        KTable<String,SynopsisAndParameters> noMergedSynopsisStream =  subSynopses.filter((key, value) -> !mergedSynopsesControrler && value.getSynopsis().getSynopsesID()!=4 && value.getSynopsis().getSynopsesID()!=9 )
                .toTable(Materialized.with(Serdes.String(), new SynopsisAndParametersSerde()));

        KTable<String,RequestStructure> queryTable = queryReqStream
                .selectKey((key,value)-> value.getStreamID()+","+value.getDataSetKey()+","+value.getPartition())
                .toTable(Materialized.with(Serdes.String(),new RequestStructureSerde()));


        KTable<String, Tuple2Dem<SynopsisAndParameters, RequestStructure>> joinedTable = noMergedSynopsisStream.join(
                queryTable,
                Tuple2Dem::new
        );
       // joinedTable.toStream().peek(((key, value) -> System.out.println("The key value"+key+","+value.getValue1().getSynopsis().size()+","+value.getValue2().getRequestID())));


        KStream<String, String> estimationNoMergedSynopsis = joinedTable
                .toStream()
                .selectKey((key,value)-> {
                   Object data = value.getValue2().getParam()[0];
                   if (data instanceof ArrayList) {
                       ArrayList<Object> dataList = (ArrayList<Object>) data;
                       data = dataList.toArray(new Object[0]);
                   }
                   return Arrays.toString((Object[])data)+","+value.getValue2().getRequestID()+","+value.getValue2().getSynopsisID()+","+value.getValue1().getParameters()[1];
               })
                .transform(() -> new WindowAggregatorForNoMergeSynopsisTransformer(Duration.ofSeconds(5),"SynopsisTable-State-Store",numThreads),"SynopsisTable-State-Store")
                .mapValues((key,value)-> {
                    String synopsesMessage;
                    synopsesMessage=defineEstimationSynopsisForNoMergable(value);
                    System.out.println("--------------------------------------------------------------------------------------");
                    System.out.println(synopsesMessage);
                    System.out.println("--------------------------------------------------------------------------------------");
                    return synopsesMessage;
                });


        singleThreadSynopsesResult//.transform(()->new TotalOutputTopicTransformer("singleThreadSynopsesResult-byte-count",synopsesType))
                .to("OutputTopicSynopsis"+SynopsisId,Produced.with(Serdes.String(), Serdes.String()));


        estimationNoMergedSynopsis.to("OutputTopicSynopsis"+SynopsisId,Produced.with(Serdes.String(), Serdes.String()));

        estimationMergedSynopsis.toStream().to("OutputTopicSynopsis"+SynopsisId,Produced.with(Serdes.String(), Serdes.String()));

        this.kafkaStreams = new KafkaStreams( builder.build(), properties);

    }

    private String defineEstimationSynopsisForNoMergable(EstimationResult estimationResult){
        String synopsesMessage = null;
        String spesificSynopsesMessage = null;
        String esimationParameterToDefine= String.valueOf(estimationResult.getEstimationParams()[1]);
        switch (estimationResult.getSynopsisID()) {

            case 5:
                if (esimationParameterToDefine.contains("FrequentItems")) {
                    synopsesMessage = "For Stock "+estimationResult.getStreamID()+" and Dataset "+estimationResult.getDatasetKey()+ "\n" +
                            " All the Frequent Item in "+ estimationResult.getField()+ " field " + "\n";
                    spesificSynopsesMessage = "LossyCounting Frequent Items in dataset are: " +estimationResult.getEstimationVal();
                } else {
                    synopsesMessage = "For Stock "+estimationResult.getStreamID()+" and Dataset "+estimationResult.getDatasetKey()+ "\n" +
                            "Estimate Count in  "+ estimationResult.getField()+ " field of value: "+estimationResult.getEstimationParams()[0].toString() + "\n";
                    spesificSynopsesMessage = "LossyCounting Result is: " + estimationResult.getEstimationVal();

                }
                break;
            case 6:
                if (esimationParameterToDefine.contains("isFrequent")) {
                    synopsesMessage = "For Stock "+estimationResult.getStreamID()+" and Dataset "+estimationResult.getDatasetKey()+ "\n" +
                            " Estimate the given frequency "+estimationResult.getEstimationParams()[0].toString()+" in "+ estimationResult.getField()+ " field  s considered \"frequent\"" + "\n";
                    boolean estimationValBool = Boolean.parseBoolean(estimationResult.getEstimationVal());
                    spesificSynopsesMessage = "StickySampling isFrequent Result: " + (estimationValBool ? "value is frequent." : "value is not frequent.");

                } else if (esimationParameterToDefine.contains("FrequentItems")) {
                    synopsesMessage = "For Stock "+estimationResult.getStreamID()+" and Dataset "+estimationResult.getDatasetKey()+ "\n" +
                            " All the Frequent Item in "+ estimationResult.getField()+ " field " + "\n";
                    spesificSynopsesMessage = "StickySampling Frequent Items in dataset are: " + estimationResult.getEstimationVal();
                } else {
                    synopsesMessage = "For Stock "+estimationResult.getStreamID()+" and Dataset "+estimationResult.getDatasetKey()+ "\n" +
                            "Estimate Count in  "+ estimationResult.getField()+ " field of value: "+estimationResult.getEstimationParams()[0].toString() + "\n";
                    spesificSynopsesMessage = "StickySampling Result is: " + estimationResult.getEstimationVal();
                }
                break;
            case 7:
                if (esimationParameterToDefine.contains("L2norm")) {
                    synopsesMessage = "For Stock "+estimationResult.getStreamID()+" and Dataset "+estimationResult.getDatasetKey()+ "\n" +
                            " Estimating L2 norm in "+ estimationResult.getField()+ " field " + "\n";
                    spesificSynopsesMessage = "AMSSketch L2 norm Result is: " +estimationResult.getEstimationVal();
                } else {
                    synopsesMessage = "For Stock "+estimationResult.getStreamID()+" and Dataset "+estimationResult.getDatasetKey()+ "\n" +
                            "Estimate Count in  "+ estimationResult.getField()+ " field of value: "+estimationResult.getEstimationParams()[0].toString() + "\n";
                    spesificSynopsesMessage = "AMSSketch Count Result is: " + estimationResult.getEstimationVal();

                }
                break;
        }
        return synopsesMessage+spesificSynopsesMessage;

    }
    private String defineEstimationSynopsis(RequestStructure request,LoadAndSaveSynopsis saveSynopsis,Synopsis synopsisInst,int topicCount,Object data){
        String synopsesName=null;
        Object estimateResult;
        String synopsesMessage=null;
        String spesificSynopsesMessage=null;
        Object dataWantEstimate;

        switch (request.getSynopsisID()) {
            case 1:
                dataWantEstimate = data;
                estimateResult = synopsisInst.estimate(data);
                synopsesName="CountMin";
                synopsesMessage = "For Stock "+request.getStreamID()+" and Dataset "+request.getDataSetKey()+ "\n" +
                        " Estimate Count in "+ request.getParam()[1]+ " field of value: "+request.getParam()[0] + "\n";
                spesificSynopsesMessage="Count Min Result is: " + estimateResult;
                break;
            case 2:
                dataWantEstimate = data;
                estimateResult = synopsisInst.estimate(data);
                synopsesName= "HyperLogLog";
                synopsesMessage = "For Stock "+request.getStreamID()+" and Dataset "+request.getDataSetKey()+ "\n" +
                        " Estimate Cardinality in "+ request.getParam()[1]+ " field"+  "\n";
                spesificSynopsesMessage="Hyper Log Log Result is: " + estimateResult;
                break;
            case 3:
                dataWantEstimate = data;
                estimateResult = synopsisInst.estimate(data);
                synopsesName= "BloomFilter";
                synopsesMessage = "For Stock "+request.getStreamID()+" and Dataset "+request.getDataSetKey()+ "\n" +
                        " Estimate Member of a Set in "+ request.getParam()[1]+ " field of value: "+request.getParam()[0] + "\n";
                spesificSynopsesMessage="Bloom Filter Result: " +((boolean)estimateResult ? "value might be present in the set." : "is definitely not present in the set.");
                break;
            case 4:
                dataWantEstimate = data;
                estimateResult = synopsisInst.estimate(data);
                synopsesName= "DFT";
                synopsesMessage = "For Stock "+request.getStreamID()+" and Dataset "+request.getDataSetKey()+ "\n" +
                        " Estimate Spectral Analysis in "+ request.getParam()[1]+ " field " + "\n";
                spesificSynopsesMessage= "DFT Result: The representation in Frequency domain is : "+ estimateResult ;
                break;
            case 5:

                estimateResult = synopsisInst.estimate(data);
                synopsesName= "LossyCounting";
                Object[] dataTuple1 = (Object[]) data;
                if (dataTuple1[1].equals("frequentItems")) {
                    List<CountEntry<Object>> frequentItems = (List<CountEntry<Object>>) estimateResult;
                    StringBuilder frequentItemsStringBuilder = new StringBuilder();
                    for (CountEntry<Object> entry : frequentItems) {
                        frequentItemsStringBuilder.append("Item: ").append(entry.item).append(", Frequency: ").append(entry.frequency).append("\n");
                    }
                    synopsesMessage = "For Stock "+request.getStreamID()+" and Dataset "+request.getDataSetKey()+ "\n" +
                            " All the Frequent Item in "+ request.getParam()[1]+ " field " + "\n";
                    spesificSynopsesMessage = "LossyCounting Frequent Items in dataset are: " +frequentItemsStringBuilder;
                } else {

                    synopsesMessage = "For Stock "+request.getStreamID()+" and Dataset "+request.getDataSetKey()+ "\n" +
                            "Estimate Count in  "+ request.getParam()[1]+ " field of value: "+dataTuple1[0].toString() + "\n";
                    spesificSynopsesMessage = "LossyCounting Result is: " + estimateResult;

                }
                break;
            case 6:
                estimateResult = synopsisInst.estimate(data);
                synopsesName= "StickySampling";
                Object[] dataTuple2 = (Object[]) data;
                if (dataTuple2[1].equals("isFrequent")) {

                    synopsesMessage = "For Stock "+request.getStreamID()+" and Dataset "+request.getDataSetKey()+ "\n" +
                            " Estimate the given frequency "+dataTuple2[0].toString()+" in "+ request.getParam()[1]+ " field  s considered \"frequent\"" + "\n";
                    spesificSynopsesMessage = "StickySampling isFrequent Result: " + ((Boolean)estimateResult ? "value is frequent." : "value is not frequent.");

                } else if (dataTuple2[1].equals("frequentItems")) {
                    List<CountEntry<Object>> frequentItems = (List<CountEntry<Object>>) estimateResult;
                    StringBuilder frequentItemsStringBuilder = new StringBuilder();
                    for (CountEntry<Object> entry : frequentItems) {
                        frequentItemsStringBuilder.append("Item: ").append(entry.item).append(", Frequency: ").append(entry.frequency).append("\n");
                    }
                    synopsesMessage = "For Stock "+request.getStreamID()+" and Dataset "+request.getDataSetKey()+ "\n" +
                            " All the Frequent Item in "+ request.getParam()[1]+ " field " + "\n";
                    spesificSynopsesMessage = "StickySampling Frequent Items in dataset are: " + frequentItemsStringBuilder;
                } else {

                    synopsesMessage = "For Stock "+request.getStreamID()+" and Dataset "+request.getDataSetKey()+ "\n" +
                            "Estimate Count in  "+ request.getParam()[1]+ " field of value: "+dataTuple2[0].toString() + "\n";
                    spesificSynopsesMessage = "StickySampling Result is: " + estimateResult;
                }
                break;
            case 7:
                Object[] dataTuple3 = (Object[]) data;
                estimateResult = synopsisInst.estimate(data);
                if (dataTuple3[1].equals("L2norm")) {

                    synopsesMessage = "For Stock "+request.getStreamID()+" and Dataset "+request.getDataSetKey()+ "\n" +
                            " Estimating L2 norm in "+ request.getParam()[1]+ " field " + "\n";
                    spesificSynopsesMessage = "AMSSketch L2 norm Result is: " +estimateResult;
                } else {

                    synopsesMessage = "For Stock "+request.getStreamID()+" and Dataset "+request.getDataSetKey()+ "\n" +
                            "Estimate Count in  "+ request.getParam()[1]+ " field of value: "+dataTuple3[0].toString() + "\n";
                    spesificSynopsesMessage = "AMSSketch Count Result is: " + estimateResult;

                }
                break;
            case 8:
                estimateResult = synopsisInst.estimate(data);
                synopsesName= "GKQuantiles";
                synopsesMessage = "For Stock "+request.getStreamID()+" and Dataset "+request.getDataSetKey()+ "\n" +
                        "Estimate the Quantile in "+ request.getParam()[1]+ " field for quantile: "+ data + "\n";
                spesificSynopsesMessage= "GKQuantiles Quantile Result is: " + estimateResult;
                break;
            case 9:
                estimateResult = synopsisInst.estimate(data);
                synopsesMessage = "For Stock "+request.getStreamID()+" and Dataset "+request.getDataSetKey()+ "\n" +
                        "Estimate BitSet in "+ request.getParam()[1]+ " field"+  "\n";
                spesificSynopsesMessage= "LSH BitSet Result is: " + estimateResult;
                break;
            default:
                throw new IllegalArgumentException("Unknown synopsesID: " + request.getSynopsisID());
        }
        //saveSynopsis.saveSynopsisToFile(synopsisInst, topicCount,synopsesName);


        return synopsesMessage+spesificSynopsesMessage;
    }
    //protected abstract void printEstimationAndReturnEstimateValueForLoad(KStream<String, Tuple2DemWithSynopsis<RequestWithDataStructure, RequestStructure>> combinedStructureAndQueryForLoad2);
  //  protected abstract KStream<String,String> printEstimationAndReturnEstimateValue(KStream<String, Tuple2Dem<SynopsisAndParameters, Synopsis>> estimationStreamForMultiThread);
    //protected abstract  KStream<String, Tuple2Dem<SynopsisAndParameters,Synopsis>> calculateTotalResultForSynopsis(KStream<String,Tuple2Dem<RequestStructure,SynopsisAndParameters>>  branchedStreams2MultiThread,KStream<String, Synopsis> loadSynopsesTopic);
    private void addStateStores(StreamsBuilder builder){

        StoreBuilder<KeyValueStore<String, Synopsis>> synopsesStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(SYNOPSES_STORE_NAME),
                        Serdes.String(),
                        new SynopsesSerdes()
                );

        builder.addStateStore(synopsesStoreBuilder);
        StoreBuilder<KeyValueStore<String, SynopsisAndParameters>> synopsesTableStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("SynopsisTable-State-Store"),
                        Serdes.String(),
                        new SynopsisAndParametersSerde()
                );
        builder.addStateStore(synopsesTableStoreBuilder);

    }

    public void start() {
        kafkaStreams.start();
    }

    public void clear() {
        kafkaStreams.cleanUp();
    }

    public void stop() {
        kafkaStreams.close();
    }
}
