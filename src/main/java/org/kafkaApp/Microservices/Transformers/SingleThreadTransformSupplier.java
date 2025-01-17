package org.kafkaApp.Microservices.Transformers;

import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.kafkaApp.Microservices.Router.LoadAndSaveSynopsis;
import org.kafkaApp.Structure.entities.RequestStructure;
import org.kafkaApp.Structure.dto.SynopsisAndParameters;
import org.kafkaApp.Structure.dto.Tuple2Dem;
import org.kafkaApp.Synopses.Synopsis;
import org.streaminer.stream.frequency.util.CountEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class SingleThreadTransformSupplier implements ValueTransformerWithKeySupplier<String, Tuple2Dem<RequestStructure, SynopsisAndParameters>, String> {
    private final String synopsisStoreName;
    private final int topicCount;

    public SingleThreadTransformSupplier(String synopsisStoreName, int topicCount) {
        this.synopsisStoreName = synopsisStoreName;
        this.topicCount = topicCount;
    }
    private static class SingleThreadTransform implements ValueTransformerWithKey<String, Tuple2Dem<RequestStructure, SynopsisAndParameters>, String> {
        private KeyValueStore<String, Synopsis> stateStore;
        private final String stateStoreName;
        private final int topicCount;
        AtomicLong AdhocCounter = new AtomicLong(0);
        public SingleThreadTransform(String stateStoreName,int topicCount) {
            this.stateStoreName = stateStoreName;
            this.topicCount=topicCount;
        }
        @Override
        public void init(ProcessorContext context) {
            stateStore = context.getStateStore(stateStoreName);
        }



        @Override
        public String transform(String key, Tuple2Dem<RequestStructure, SynopsisAndParameters> value) {
            String [] splitnewKey = key.split(",");
            String newKey = splitnewKey[0]+","+splitnewKey[1];
            Synopsis synopsisInst = stateStore.get(newKey);
            if(AdhocCounter.get()>1 && value.getValue1().getParam()[3].equals("Ad-hoc")){
                return null;
            }
            AdhocCounter.incrementAndGet();

            String fieldParameter = (String) value.getValue1().getParam()[1];
            LoadAndSaveSynopsis loadAndSaveSynopsis = new LoadAndSaveSynopsis();


            String synopsesMessage=null;

            Object data = value.getValue1().getParam()[0];
            if (data instanceof ArrayList) {
                ArrayList<Object> dataList = (ArrayList<Object>) data;
                data = dataList.toArray(new Object[0]);
            }
            System.out.println("The Size of "+synopsisInst.size());
            synopsesMessage=defineEstimationSynopsis(value.getValue1(),loadAndSaveSynopsis,synopsisInst,topicCount,data);

            System.out.println("--------------------------------------------------------------------------------------");
            System.out.println(synopsesMessage);
            System.out.println("--------------------------------------------------------------------------------------");
            return synopsesMessage;
        }

        @Override
        public void close() {
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
                    spesificSynopsesMessage= "GKQuantiles quantile Result is: " + estimateResult;
                    break;
                case 9:
                    estimateResult = synopsisInst.estimate(data);
                    synopsesMessage = "For Stock "+request.getStreamID()+" and Dataset "+request.getDataSetKey()+ "\n" +
                            "Estimate BitSet in "+ request.getParam()[1]+ " field for quantile: "+ data + "\n";
                    spesificSynopsesMessage= "LSH BitSet Result is: " + estimateResult;
                    break;
                case 10:
                    estimateResult = synopsisInst.estimate(data);
                    synopsesName= "WindowSketchQuantilesSynopsis";
                    synopsesMessage = "For Stock "+request.getStreamID()+" and Dataset "+request.getDataSetKey()+ "\n" +
                            "Estimate the Quantile in "+ request.getParam()[1]+ " field for quantile: "+ data + "\n";
                    spesificSynopsesMessage= "WindowSketchQuantiles quantile Result is: " + estimateResult;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown synopsesID: " + request.getSynopsisID());
            }
            //saveSynopsis.saveSynopsisToFile(synopsisInst, topicCount,synopsesName);


            return synopsesMessage+spesificSynopsesMessage;
        }


    }
    @Override
    public ValueTransformerWithKey<String, Tuple2Dem<RequestStructure, SynopsisAndParameters>, String> get() {
        return new SingleThreadTransform(synopsisStoreName,topicCount);
    }
}
