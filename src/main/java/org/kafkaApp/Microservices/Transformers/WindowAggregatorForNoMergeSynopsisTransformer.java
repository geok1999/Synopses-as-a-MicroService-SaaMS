package org.kafkaApp.Microservices.Transformers;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;
import org.kafkaApp.Structure.result.EstimationResult;
import org.kafkaApp.Structure.entities.RequestStructure;
import org.kafkaApp.Structure.dto.SynopsisAndParameters;
import org.kafkaApp.Structure.dto.Tuple2Dem;
import org.streaminer.stream.frequency.util.CountEntry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class WindowAggregatorForNoMergeSynopsisTransformer implements Transformer<String, Tuple2Dem<SynopsisAndParameters, RequestStructure>, KeyValue<String, EstimationResult>> {

    private KeyValueStore<String, SynopsisAndParameters> stateStoreSynopsisAndParameters;
    private String stateStoreName;
    private final long durationMillis;
    private int partitionNum;
    private String newKey;
    private long estimationVal;
    private long estimationL2Val;
    Boolean estimationFrequentItem = null;

    private HashSet<CountEntry<Object>> totalFrequentItems =  new HashSet<>();
    private HashMap<Object, CountEntry<Object>> totalFrequentItemsAgg = new HashMap<>();


    private ConcurrentHashMap<String, AtomicInteger> adhocRequestCountMap;


    public WindowAggregatorForNoMergeSynopsisTransformer(Duration duration, String stateStoreName, int partitionNum) {
        this.durationMillis = duration.toMillis();
        this.stateStoreName = stateStoreName;
        this.partitionNum = partitionNum;
        this.adhocRequestCountMap = new ConcurrentHashMap<>();


    }

    @Override
    public void init(ProcessorContext context) {
        //stateStore = context.getStateStore("ESTIMATE_SYNOPSES_STORE_NAME");
        stateStoreSynopsisAndParameters = context.getStateStore("SynopsisTable-State-Store");

        context.schedule(Duration.ofMillis(durationMillis), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            if(newKey!=null){
                String[] keySplit = newKey.split(",");

                String keySplit0 = keySplit[0].replace("[", "").replace("]", "");
                String keySplit1 = keySplit[1].replace("[", "").replace("]", "");
                Object[] estimateObj = new Object[]{Double.parseDouble(keySplit0), keySplit1};
                int requestIDquery = Integer.parseInt(keySplit[2]);
               // int partition = Integer.parseInt(keySplit[3]);
                int synopsisID = Integer.parseInt(keySplit[3]);

                String aggregateKey = keySplit[0]+","+keySplit[1]+","+keySplit[2]+","+synopsisID;
                EstimationResult estimationResult = null;
                boolean checkifEstimateZero=false;
                boolean checkifEstimateF2Zero=false;
                boolean checkifEstimateForFrequentItemsZero=false;
                List<CountEntry<Object>> result = new ArrayList<>();
                long elementsCounted=0;
                String[] parameters = new String[0];
                Object[] synopsisParams = new Object[0];



                for (int i = 0; i < this.partitionNum; i++) {
                    SynopsisAndParameters synopsisAndParameters = stateStoreSynopsisAndParameters.get(aggregateKey+","+i);
                    if(synopsisAndParameters.getSynopsis()!=null){
                        elementsCounted+= synopsisAndParameters.getSynopsis().size();
                        parameters = synopsisAndParameters.getSynopsis().getSynopsisParameters().split(",");
                        synopsisParams = synopsisAndParameters.getParameters();
                        if (keySplit1.contains("CountEstimation")) {
                            estimationVal += (Long) synopsisAndParameters.getSynopsis().estimate(estimateObj);
                            if (estimationVal == 0)
                                checkifEstimateZero = true;
                        } else if (keySplit1.contains("FrequentItems")) {
                            List<CountEntry<Object>> frequentItems = (List<CountEntry<Object>>) synopsisAndParameters.getSynopsis().estimate(estimateObj);
                            //System.out.println("Size"+requestIDquery+", "+frequentItems.size());
                            for (CountEntry<Object> entry : frequentItems) {
                                Object item = entry.item;
                                long frequency = entry.frequency;
                                if (totalFrequentItemsAgg.containsKey(item)) {
                                    CountEntry<Object> existingEntry = totalFrequentItemsAgg.get(item);
                                    existingEntry.frequency += frequency;
                                    totalFrequentItemsAgg.put(item, new CountEntry<Object>(item, existingEntry.frequency));
                                } else {
                                    totalFrequentItemsAgg.put(item, new CountEntry<Object>(item, frequency));
                                }
                            }

                            if (totalFrequentItemsAgg.isEmpty())
                                checkifEstimateForFrequentItemsZero = true;
                        } else if (keySplit1.contains("isFrequent")) {
                            long valueCheckFreq = Long.parseLong(keySplit0);
                            estimationFrequentItem = valueCheckFreq >= (Double.parseDouble(parameters[0]) - Double.parseDouble(parameters[1]) * elementsCounted);
                        }
                        else if (keySplit1.contains("L2norm")) {
                            estimationL2Val += (Long) synopsisAndParameters.getSynopsis().estimate(estimateObj);
                            if (estimationL2Val == 0)
                                checkifEstimateF2Zero = true;
                        }
                    }
                }
                if (estimationVal != 0 ||checkifEstimateZero) {
                    context.forward(Integer.toString(requestIDquery), new EstimationResult(Long.toString(estimationVal), synopsisID,estimateObj, (String) synopsisParams[5], (String) synopsisParams[6], (String) synopsisParams[3]));
                    estimationVal=0;
                    newKey=null;
                }
                if (!totalFrequentItemsAgg.isEmpty() || checkifEstimateForFrequentItemsZero){

                    // removing unecessery elements
                    double minSupport = Double.parseDouble(keySplit0);
                    if(synopsisID==5){
                        for (CountEntry<Object> entry : totalFrequentItemsAgg.values()) {
                            if (entry.frequency >= (minSupport - Double.parseDouble(parameters[0])) * elementsCounted) {
                                result.add(entry);
                            }
                        }
                    }else if(synopsisID==6){
                        for (CountEntry<Object> entry : totalFrequentItemsAgg.values()) {
                            if (entry.frequency >= (Double.parseDouble(parameters[0]) - Double.parseDouble(parameters[1]) * elementsCounted)){
                                result.add(entry);
                            }
                        }
                    }

                    StringBuilder frequentItemsStringBuilder = new StringBuilder();
                    for (CountEntry<Object> entry : result) {
                        frequentItemsStringBuilder.append("Item: ").append(entry.item).append(", Frequency: ").append(entry.frequency).append("\n");
                    }

                    context.forward(Integer.toString(requestIDquery), new EstimationResult(frequentItemsStringBuilder.toString(), synopsisID,estimateObj,(String) synopsisParams[5], (String) synopsisParams[6], (String) synopsisParams[3]));
                    totalFrequentItemsAgg.clear();
                    newKey=null;
                }
                if (estimationFrequentItem != null) {
                    context.forward(Integer.toString(requestIDquery), new EstimationResult(estimationFrequentItem.toString(), synopsisID,estimateObj,(String) synopsisParams[5], (String) synopsisParams[6], (String) synopsisParams[3]));

                    estimationFrequentItem=null;
                    newKey=null;
                }
                if (estimationL2Val != 0 ||checkifEstimateF2Zero) {
                    context.forward(Integer.toString(requestIDquery), new EstimationResult(Long.toString(estimationL2Val), synopsisID,estimateObj,(String) synopsisParams[5], (String) synopsisParams[6], (String) synopsisParams[3]));
                    estimationL2Val=0;
                    newKey=null;
                }
            }


        });
    }

    @Override
    public KeyValue<String, EstimationResult> transform(String key,  Tuple2Dem<SynopsisAndParameters, RequestStructure> value) {
        stateStoreSynopsisAndParameters.put(key,value.getValue1());
        if ("Ad-hoc".equals(value.getValue2().getParam()[3])) {
            AtomicInteger count = adhocRequestCountMap.getOrDefault(Integer.toString(value.getValue2().getRequestID()), new AtomicInteger(0));
            //System.out.println("AtomicInteger value for request ID " + value.getValue2().getRequestID() + ": " + count.get());
            if (count.get() > 0) {
             //   newKey=null;
                return null;
            } else {
                count.incrementAndGet();
                adhocRequestCountMap.put(Integer.toString(value.getValue2().getRequestID()),count);
            }
        }
        newKey=key;
        return null; // We only emit data in the punctuate method
    }
    @Override
    public void close() {

    }
}