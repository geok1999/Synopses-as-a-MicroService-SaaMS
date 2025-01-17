package org.kafkaApp.Serdes.Init.RequestWithDataStructure;


import org.kafkaApp.Serdes.GeneralFormat.GeneralDeserializer;
import org.kafkaApp.Structure.dto.RequestWithDataStructure;


public class RequestWithDataStructureDeserializer extends GeneralDeserializer<RequestWithDataStructure> {


    public RequestWithDataStructureDeserializer(Class<RequestWithDataStructure> type) {
        super(type);
    }
}
