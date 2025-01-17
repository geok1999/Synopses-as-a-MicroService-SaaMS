package org.kafkaApp.Serdes.Init.ListRequestStructure;


import com.fasterxml.jackson.core.type.TypeReference;
import org.kafkaApp.Serdes.GeneralFormat.GeneralDeserializer;
import org.kafkaApp.Structure.entities.RequestStructure;

import java.util.List;

public class ListRequestStructureDeserializer extends GeneralDeserializer<List<RequestStructure>> {


    public ListRequestStructureDeserializer(TypeReference<List<RequestStructure>> type) {
        super(type);
    }
}
