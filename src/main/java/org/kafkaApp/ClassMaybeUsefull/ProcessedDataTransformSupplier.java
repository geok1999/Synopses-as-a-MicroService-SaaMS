package org.kafkaApp.ClassMaybeUsefull;

import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.kafkaApp.Structure.*;

import java.util.ArrayList;
import java.util.List;

public class ProcessedDataTransformSupplier implements ValueTransformerWithKeySupplier<String, Tuple2Dem<List<DataStructure>, StructureWithMetaDataParameters<RequestStructure>>, Tuple2DemExt<List<DataStructure>,StructureWithMetaDataParameters<RequestStructure>>> {


    private static class ProcessedDataTransform implements ValueTransformerWithKey<String, Tuple2Dem<List<DataStructure>,StructureWithMetaDataParameters<RequestStructure>>, Tuple2DemExt<List<DataStructure>,StructureWithMetaDataParameters<RequestStructure>>> {
        private KeyValueStore<String, List<String>> stateStore;

        @Override
        public void init(ProcessorContext context) {
            stateStore = context.getStateStore("ProcessDataStore");
        }

        @Override
        public Tuple2DemExt<List<DataStructure>,StructureWithMetaDataParameters<RequestStructure>> transform(String readOnlyKey, Tuple2Dem<List<DataStructure>,StructureWithMetaDataParameters<RequestStructure>> value) {
            RequestStructure request = value.getValue2().getReq();
            String field = request.getParam()[1].toString();
            String newKey = request.getStreamID()+","+request.getDataSetKey()+","+Integer.valueOf(request.getSynopsisID()).toString()+","+request.getParam()[1].toString();

            String currentFirstObjectID = value.getValue1().get(0).getObjectID();

            List<String> listOfFirstObjectsID = stateStore.get(newKey);

            if(listOfFirstObjectsID == null){
                listOfFirstObjectsID = new ArrayList<>();
            }

            boolean isProcessed = checkIfObjectIDExistInList(listOfFirstObjectsID, currentFirstObjectID);

            if(!isProcessed) {
                // The object ID has not been processed before, add it to the list
                listOfFirstObjectsID.add(currentFirstObjectID);
                stateStore.put(newKey, listOfFirstObjectsID);
            }

            return new Tuple2DemExt<>(value.getValue1(), value.getValue2(), isProcessed ? "true" : "false");
        }

        private boolean checkIfObjectIDExistInList(List<String> listOfObjectId, String targetObjectID) {
            if(listOfObjectId.isEmpty()) return false;

            for (String objectID : listOfObjectId) {
                if(objectID.equals(targetObjectID)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void close() {

        }
    }
    @Override
    public ValueTransformerWithKey<String, Tuple2Dem<List<DataStructure>,StructureWithMetaDataParameters<RequestStructure>>, Tuple2DemExt<List<DataStructure>,StructureWithMetaDataParameters<RequestStructure>>> get() {
        return new ProcessedDataTransform();
    }
}
