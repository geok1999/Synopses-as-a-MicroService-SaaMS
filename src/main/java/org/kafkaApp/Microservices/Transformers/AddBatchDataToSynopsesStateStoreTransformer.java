package org.kafkaApp.Microservices.Transformers;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;
import org.kafkaApp.Structure.DataStructure;
import org.kafkaApp.Structure.SynopsisAndParameters;
import org.kafkaApp.Synopses.Synopsis;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class AddBatchDataToSynopsesStateStoreTransformer implements Transformer<String, DataStructure, KeyValue<String, SynopsisAndParameters>> {
    private final long durationMillis;
    private KeyValueStore<String, Synopsis> synopsisStore;
    private String synopsisStoreName;
    private List<DataStructure> localBuffer = new ArrayList<>();
    private String newKey;


    public AddBatchDataToSynopsesStateStoreTransformer(Duration duration, String synopsisStoreName ) {
        this.durationMillis = duration.toMillis();
        this.synopsisStoreName = synopsisStoreName;
    }

    @Override
    public void init(ProcessorContext context) {
        synopsisStore = context.getStateStore(synopsisStoreName);


        context.schedule(Duration.ofMillis(durationMillis), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            /*if (!localBuffer.isEmpty()) {
                batchStore.put("batchKey", new ArrayList<>(localBuffer));
                localBuffer.clear();
            }*/
           // List<DataStructure> batchedData = batchStore.get("batchKey");

            if (!localBuffer.isEmpty()) {
               // System.out.println("Thread i am"+ Thread.currentThread());
                String key = newKey;//localBuffer.get(0).getStreamID() + "," + localBuffer.get(0).getDataSetKey();
                Synopsis synopsis = synopsisStore.get(key);
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

                localBuffer.clear();
            }
        });
        // New punctuator for flushing localBuffer to batchStore

    }

    @Override
    public KeyValue<String, SynopsisAndParameters> transform(String key, DataStructure value) {
        localBuffer.add(value);
        newKey=key;

        //if (localBuffer.size() >= BUFFER_FLUSH_SIZE) {  // Assume BUFFER_FLUSH_SIZE is a predefined constant
            //batchStore.put("batchKey", new ArrayList<>(localBuffer));
            //localBuffer.clear();
        //}
        return null;

    }
    @Override
    public void close() {

    }
    private void updateSynopsis(Synopsis synopsis, List<DataStructure> batchedData) {
        // Extracting the field to build synopsis on from the synopsis details
        String[] synopsesParameters = synopsis.getSynopsisDetails().split(",");
        String fieldBuildSynopses = synopsesParameters[3];

        if (fieldBuildSynopses.equals("price")) {
            batchedData.forEach(data -> synopsis.add(data.getPrice()));
        } else if (fieldBuildSynopses.equals("volume")) {
            batchedData.forEach(data -> synopsis.add(data.getVolume()));
        }
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
