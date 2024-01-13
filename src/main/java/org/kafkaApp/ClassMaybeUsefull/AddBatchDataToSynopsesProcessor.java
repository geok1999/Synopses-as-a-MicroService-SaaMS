package org.kafkaApp.ClassMaybeUsefull;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;
import org.kafkaApp.Structure.DataStructure;
import org.kafkaApp.Structure.SynopsisAndParameters;
import org.kafkaApp.Synopses.Synopsis;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

    /* builder.build().addSource("Source", new StringDeserializer(), new DataStructureDeserializer(), dataTopicName)
                .addProcessor("Process", () -> new AddBatchDataToSynopsesProcessor(Duration.ofSeconds(1),SYNOPSES_STORE_NAME), "Source")
                .connectProcessorAndStateStores("Process",SYNOPSES_STORE_NAME)
                .addSink("Sink", intermidiateTopicName,new StringSerializer(),new SynopsisAndParametersSerializer(), "Process");*/


public class AddBatchDataToSynopsesProcessor implements Processor<String, DataStructure> {
    private ProcessorContext context;
    private Duration durationMillis;
    private KeyValueStore<String, Synopsis> synopsisStore;
    private KeyValueStore<String, List<DataStructure>> batchStore;
    private String synopsisStoreName;

    private List<DataStructure> localBuffer = new ArrayList<>();



    public AddBatchDataToSynopsesProcessor(Duration duration, String synopsisStoreName) {
        this.durationMillis = duration;
        this.synopsisStoreName = synopsisStoreName;

    }

    @Override
    public void init(ProcessorContext context) {
        synopsisStore = context.getStateStore(synopsisStoreName);
        //batchStore = context.getStateStore(batchStoreName);

        context.schedule(durationMillis, PunctuationType.WALL_CLOCK_TIME, timestamp -> {
           /* if (!localBuffer.isEmpty()) {
                batchStore.put("batchKey", new ArrayList<>(localBuffer));
                localBuffer.clear();*/
         //  }
           // List<DataStructure> batchedData = batchStore.get("batchKey");
            if ( !localBuffer.isEmpty()) {
                String key = localBuffer.get(0).getStreamID() + "," + localBuffer.get(0).getDataSetKey();

                Synopsis synopsis = synopsisStore.get(key);
                while (synopsis==null){
                    if (synopsis!=null)
                        break;
                    System.out.println("synopsis is null");
                }

                String[] synopsesParameters = synopsis.getSynopsisDetails().split(",");
                String fieldBuildSynopses = synopsesParameters[3];

                if (fieldBuildSynopses.equals("price")) {
                    localBuffer.parallelStream().forEachOrdered(data -> synopsis.add(data.getPrice()));
                } else if (fieldBuildSynopses.equals("volume")) {
                    localBuffer.parallelStream().forEachOrdered(data -> synopsis.add(data.getVolume()));
                }

                //updateSynopsis(synopsis, batchedData);
                synopsisStore.put(key, synopsis);

                SynopsisAndParameters sap = createSynopsisAndParameters(synopsis, localBuffer);
                context.forward(key + "," + sap.getParameters()[1], sap);

               // batchStore.put("batchKey", new ArrayList<>());  // Clearing the batched data.
                localBuffer.clear();
            }
            context.commit();
        });

    }

    @Override
    public void process(String key, DataStructure value) {
        localBuffer.add(value);
    }

    @Override
    public void close() {

    }

    private SynopsisAndParameters createSynopsisAndParameters(Synopsis synopsis, List<DataStructure> batchedData) {
        // Extracting synopsis parameters
        String[] synopsesParameters = synopsis.getSynopsisDetails().split(",");

        // Preparing parameters for SynopsisAndParameters
        Object[] parameters = new Object[7];
        parameters[0] = synopsesParameters[0]; // requestID
        parameters[1] = synopsesParameters[1]; // partitionID
        parameters[2] = Integer.parseInt(synopsesParameters[2]); // numberOfProcId
        parameters[3] = synopsesParameters[3]; // field
        parameters[4] = synopsesParameters[4]; // uid
        parameters[5] = synopsesParameters[5]; // streamID
        parameters[6] = synopsesParameters[6]; // DataSetKey

        return new SynopsisAndParameters(synopsis, parameters,0);
    }
}
/*
package org.kafkaApp.Microservices.SynopseMicroservice;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;
import org.kafkaApp.Structure.DataStructure;
import org.kafkaApp.Structure.SynopsisAndParameters;
import org.kafkaApp.Synopses.Synopsis;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;


public class AddBatchDataToSynopsesProcessor implements Processor<String, DataStructure> {
    private ProcessorContext context;
    private Duration durationMillis;
    private KeyValueStore<String, Synopsis> synopsisStore;
    private KeyValueStore<String, List<DataStructure>> batchStore;
    private String synopsisStoreName;
    private String batchStoreName;
    private List<DataStructure> localBuffer = new ArrayList<>();


    public static final int BUFFER_FLUSH_SIZE = 100;
    public AddBatchDataToSynopsesProcessor(Duration duration, String synopsisStoreName, String batchStoreName) {
        this.durationMillis = duration;
        this.synopsisStoreName = synopsisStoreName;
        this.batchStoreName = batchStoreName;
    }

    @Override
    public void init(ProcessorContext context) {
        synopsisStore = context.getStateStore(synopsisStoreName);
        batchStore = context.getStateStore(batchStoreName);

        context.schedule(durationMillis, PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            if (!localBuffer.isEmpty()) {
                batchStore.put("batchKey", new ArrayList<>(localBuffer));
                localBuffer.clear();
           }
            List<DataStructure> batchedData = batchStore.get("batchKey");
            if (batchedData != null && !batchedData.isEmpty()) {
                String key = batchedData.get(0).getStreamID() + "," + batchedData.get(0).getDataSetKey();

                Synopsis synopsis = synopsisStore.get(key);
                while (synopsis==null){
                    if (synopsis!=null)
                        break;
                    System.out.println("synopsis is null");
                }

                String[] synopsesParameters = synopsis.getSynopsisDetails().split(",");
                String fieldBuildSynopses = synopsesParameters[3];

                if (fieldBuildSynopses.equals("price")) {
                    batchedData.parallelStream().forEachOrdered(data -> synopsis.add(data.getPrice()));
                } else if (fieldBuildSynopses.equals("volume")) {
                    batchedData.parallelStream().forEachOrdered(data -> synopsis.add(data.getVolume()));
                }

                //updateSynopsis(synopsis, batchedData);
                synopsisStore.put(key, synopsis);

                SynopsisAndParameters sap = createSynopsisAndParameters(synopsis, batchedData);
                context.forward(key + "," + sap.getParameters()[1], sap);

                batchStore.put("batchKey", new ArrayList<>());  // Clearing the batched data.
                batchedData.clear();
            }
            context.commit();
        });

    }

    @Override
    public void process(String key, DataStructure value) {
        localBuffer.add(value);
    }

    @Override
    public void close() {

    }

    private SynopsisAndParameters createSynopsisAndParameters(Synopsis synopsis, List<DataStructure> batchedData) {
        // Extracting synopsis parameters
        String[] synopsesParameters = synopsis.getSynopsisDetails().split(",");

        // Preparing parameters for SynopsisAndParameters
        Object[] parameters = new Object[7];
        parameters[0] = synopsesParameters[0]; // requestID
        parameters[1] = synopsesParameters[1]; // partitionID
        parameters[2] = Integer.parseInt(synopsesParameters[2]); // numberOfProcId
        parameters[3] = synopsesParameters[3]; // field
        parameters[4] = synopsesParameters[4]; // uid
        parameters[5] = synopsesParameters[5]; // streamID
        parameters[6] = synopsesParameters[6]; // DataSetKey

        return new SynopsisAndParameters(synopsis, parameters,0);
    }
}
 */