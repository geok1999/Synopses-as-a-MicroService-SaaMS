package org.kafkaApp.Microservices.Transformers;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;
import org.kafkaApp.Microservices.Router.LoadAndSaveSynopsis;
import org.kafkaApp.Structure.SynopsisAndParameters;
import org.kafkaApp.Synopses.Synopsis;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class WindowAggregatorForMergeTransformer implements Transformer<String, SynopsisAndParameters, KeyValue<String, SynopsisAndParameters>> {
    private final Map<String, SynopsisAndParameters> tableWithSynopses = new HashMap<>();
    private KeyValueStore<String, SynopsisAndParameters> stateStore;
    private String stateStoreName;
    private final long durationMillis;
    private String newKey;
    private String oldKey;
    private int partitionNum;
    private String keyOfSynopsis;
    private String SynopsisType;
    private LoadAndSaveSynopsis loadAndSaveSynopsis;
    public WindowAggregatorForMergeTransformer(Duration duration, String stateStoreName,int partitionNum,String keyOfSynopsis,String SynopsisType) {
        this.durationMillis = duration.toMillis();
        this.stateStoreName = stateStoreName;
        this.partitionNum = partitionNum;
        this.loadAndSaveSynopsis = new LoadAndSaveSynopsis();
        this.keyOfSynopsis=keyOfSynopsis;
        this.SynopsisType=SynopsisType;
    }




    @Override
    public void init(ProcessorContext context) {
        stateStore = context.getStateStore(this.stateStoreName);

        context.schedule(Duration.ofMillis(durationMillis), PunctuationType.WALL_CLOCK_TIME, timestamp -> {

            if (!tableWithSynopses.isEmpty()) {
                stateStore.put("batchKey", tableWithSynopses.get(oldKey));
                tableWithSynopses.clear();
            }

            if (newKey != null) {
                SynopsisAndParameters merged = null;
                for (int i = 0; i < this.partitionNum; i++) {
                    String key = newKey + "," + i;
                    SynopsisAndParameters synopsisAndParameters = stateStore.get(key);
                    if (synopsisAndParameters != null) {
                        //   System.out.println("synopsisAndParameters: " + oldKey + "," + synopsisAndParameters.getSynopsis().size());
                        if (merged == null) {
                            merged = synopsisAndParameters;
                        } else {
                            Synopsis mergedSynopsis = merged.getSynopsis().merge(synopsisAndParameters.getSynopsis());
                            mergedSynopsis.setSynopsesID(merged.getSynopsis().getSynopsesID());
                            mergedSynopsis.setSynopsisDetails(merged.getSynopsis().getSynopsisDetails());
                            mergedSynopsis.setSynopsisParameters(merged.getSynopsis().getSynopsisParameters());
                            merged.setSynopsis(mergedSynopsis);


                        }
                        //  System.out.println("synopsisAndParametersMerged: " + "," + merged.getSynopsis().size());
                    }
                }

                if (merged != null) {
                    context.forward(newKey, merged);
                    loadAndSaveSynopsis.saveSynopsisToFile(merged.getSynopsis(),keyOfSynopsis,SynopsisType);
                    newKey = null;  // Reset newKey after forwarding.
                }
            }
        });


        /*context.schedule(Duration.ofMillis(durationMillis), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            if (newKey != null) {
                SynopsisAndParameters merged = null;
                for (int i = 0; i < this.partitionNum; i++) {
                    String key = newKey + "," + i;
                    SynopsisAndParameters synopsisAndParameters = stateStore.get(key);
                    if (synopsisAndParameters != null) {
                     //   System.out.println("synopsisAndParameters: " + oldKey + "," + synopsisAndParameters.getSynopsis().size());
                        if (merged == null) {
                            merged = synopsisAndParameters;
                        } else {
                            Synopsis mergedSynopsis = merged.getSynopsis().merge(synopsisAndParameters.getSynopsis());
                            merged.setSynopsis(mergedSynopsis);
                        }
                      //  System.out.println("synopsisAndParametersMerged: " + "," + merged.getSynopsis().size());
                    }
                }

                if (merged != null) {
                    context.forward(newKey, merged);
                }
            }
        });*/


         /*   KeyValueIterator<String, SynopsisAndParameters> iter = this.stateStore.all();

            while (iter.hasNext()) {
                KeyValue<String, SynopsisAndParameters> entry = iter.next();
                SynopsisAndParameters synopsisAndParameters = entry.value;
                System.out.println("synopsisAndParameters: "+oldKey+","+synopsisAndParameters.getSynopsis().size());
                if (merged == null) {
                    merged = synopsisAndParameters;
                } else {
                    Synopsis mergedSynopsis = merged.getSynopsis().merge(synopsisAndParameters.getSynopsis());
                    merged.setSynopsis(mergedSynopsis);
                }
                System.out.println("synopsisAndParametersMerged: "+","+merged.getSynopsis().size());
            }

            iter.close();*/


    }

    @Override
    public KeyValue<String, SynopsisAndParameters> transform(String key, SynopsisAndParameters value) {
        tableWithSynopses.put(key, value);
        oldKey = key;
        String[] keySplit = key.split(",");
        newKey = keySplit[0]+","+keySplit[1];
        stateStore.put(key, value);
        return null; // We only emit data in the punctuate method, not during regular transform.
    }

    @Override
    public void close() {

    }

}



// Schedule the punctuate method to be called periodically
        /*context.schedule(Duration.ofMillis(durationMillis), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            if (!tableWithSynopses.isEmpty()) {
                SynopsisAndParameters merged = null;
                for (String key : tableWithSynopses.keySet()) {
                    SynopsisAndParameters synopsisAndParameters = tableWithSynopses.get(key);
                    System.out.println("synopsisAndParameters: "+key+","+synopsisAndParameters.getSynopsis().size());
                    if (merged == null) {
                        merged = synopsisAndParameters;
                    } else {
                        Synopsis mergedSynopsis = merged.getSynopsis().merge(synopsisAndParameters.getSynopsis());
                        System.out.println("synopsisAndParametersMerged: "+key+","+mergedSynopsis.size());
                        merged.setSynopsis(mergedSynopsis);
                    }
                }
                context.forward(newKey, merged);  // Forwarding the merged result
            }
        });*/