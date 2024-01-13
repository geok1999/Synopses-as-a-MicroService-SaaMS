package org.kafkaApp.Serdes.Init.RequestStructure;


import org.kafkaApp.Serdes.GeneralFormat.GeneralDeserializer;
import org.kafkaApp.Structure.RequestStructure;

public class RequestStructureDeserializer extends GeneralDeserializer<RequestStructure> {


    public RequestStructureDeserializer(Class<RequestStructure> type) {
        super(type);
    }
}
