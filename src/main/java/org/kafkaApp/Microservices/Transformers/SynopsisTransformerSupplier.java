package org.kafkaApp.Microservices.Transformers;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.kafkaApp.Structure.RequestStructure;
import org.kafkaApp.Structure.SynopsisAndRequest;
import org.kafkaApp.Synopses.AMSSketch.AMSSketchSynopsis;
import org.kafkaApp.Synopses.BloomFilter.BloomFilterSynopsis;
import org.kafkaApp.Synopses.CountMin;
import org.kafkaApp.Synopses.DFT.DFTSynopsis;
import org.kafkaApp.Synopses.GKQuantiles.GKQuantilesSynopsis;
import org.kafkaApp.Synopses.HyperLogLog.HyperLogLogSynopsis;
import org.kafkaApp.Synopses.LSH.LSHsynopsis;
import org.kafkaApp.Synopses.LossyCounting.LossyCountingSynopsis;
import org.kafkaApp.Synopses.StickySampling.StickySamplingSynopsis;
import org.kafkaApp.Synopses.Synopsis;
import org.kafkaApp.Synopses.WindowSketchQuantiles.WindowSketchQuantilesSynopsis;

import java.util.Arrays;
import java.util.stream.Collectors;

public class SynopsisTransformerSupplier implements TransformerSupplier<String, RequestStructure, KeyValue<String, SynopsisAndRequest>> {

    private final String synopsisStoreName;

    public SynopsisTransformerSupplier(String synopsisStoreName) {
        this.synopsisStoreName = synopsisStoreName;
    }

    private static class SynopsisTransformer implements Transformer<String, RequestStructure, KeyValue<String, SynopsisAndRequest>> {

        private KeyValueStore<String, Synopsis> stateStore;
        private final String stateStoreName;

        public SynopsisTransformer(String stateStoreName) {
            this.stateStoreName = stateStoreName;
        }

        @Override
        public void init(ProcessorContext context) {
            stateStore = context.getStateStore(this.stateStoreName);
        }

        @Override
        public KeyValue<String, SynopsisAndRequest> transform(String key, RequestStructure value) {

            Object[] parameters = value.getParam();

          /*  if(keyOfLastRequest==null && lastRequestId==-1){
                keyOfLastRequest=s;
                lastRequestId=value.getRequestID();
            }
            else if(keyOfLastRequest.equals(s) && parameters[2].equals("NotQueryable")){
                System.err.println("You Can Construct Synopses because already Exists!!!!!");
                return null;
            }*/


           /* if(parameters[0].equals("LOAD_REQUEST"))
                return value;*/
            /*if((parameters[2].equals("NotQueryable") && existingCountMin!=null) ){
                System.out.println("param ++" );
                return null;
            }
            else if (parameters[2].equals("Queryable") && existingCountMin != null){
                String result = Arrays.stream(parameters)
                        .skip(3)
                        .map(String::valueOf)
                        .collect(Collectors.joining(","));
                System.out.println("result ++"+result +","+existingCountMin.getSynopsisParameters());
                /*if(!existingCountMin.getSynopsisParameters().equals(result)){

                    context.forward(record.withValue(null));
                    return;
                }

            }*/
            String newKey = value.getStreamID() + "," + value.getDataSetKey();
            Synopsis synopsis = null;

            if (parameters[2].equals("NotQueryable")) {
                String[] synopsisInfo = new String[3];

                synopsisInfo[0] = Integer.toString(value.getSynopsisID());

                String result = Arrays.stream(parameters)
                        .skip(3)
                        .map(String::valueOf)
                        .collect(Collectors.joining(","));

                synopsisInfo[1] = value.getRequestID() + "," + value.getPartition() + ","
                        + value.getNoOfP() + "," + parameters[1] + "," + value.getUid() + "," + value.getStreamID() + "," + value.getDataSetKey();

                synopsisInfo[2] = result;
                System.out.println("param " + synopsisInfo[2]);

                System.out.println("State Store is null??? " + stateStore);

                System.out.println("newKey: " + value.getPartition() + "," + value.getRequestID());
                switch (value.getSynopsisID()) {
                    case 1:
                        synopsis = new CountMin(synopsisInfo);
                        break;
                    case 2:
                        synopsis = new HyperLogLogSynopsis(synopsisInfo);
                        break;
                    case 3:
                        synopsis = new BloomFilterSynopsis(synopsisInfo);
                        break;
                    case 4:
                        synopsis = new DFTSynopsis(synopsisInfo);
                        break;
                    case 5:
                        synopsis = new LossyCountingSynopsis(synopsisInfo);
                        break;
                    case 6:
                        synopsis = new StickySamplingSynopsis(synopsisInfo);
                        break;
                    case 7:
                        synopsis = new AMSSketchSynopsis(synopsisInfo);
                        break;
                    case 8:
                        synopsis = new GKQuantilesSynopsis(synopsisInfo);
                        break;
                    case 9:
                        synopsis = new LSHsynopsis(synopsisInfo);
                        break;
                    case 10:
                        synopsis = new WindowSketchQuantilesSynopsis(synopsisInfo);
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown synopsesID: " + value.getSynopsisID());
                }
                stateStore.put(newKey, synopsis);
                System.out.println(stateStore.approximateNumEntries());

            }

            return new KeyValue<>(newKey, new SynopsisAndRequest(synopsis, value));
        }

        @Override
        public void close() {
            // ... your existing close logic
        }
    }

    @Override
    public Transformer<String, RequestStructure, KeyValue<String, SynopsisAndRequest>> get() {
        return new SynopsisTransformer(this.synopsisStoreName);
    }
}
