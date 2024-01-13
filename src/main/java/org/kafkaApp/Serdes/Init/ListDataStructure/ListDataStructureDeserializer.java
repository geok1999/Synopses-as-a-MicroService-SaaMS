package org.kafkaApp.Serdes.Init.ListDataStructure;


import com.fasterxml.jackson.core.type.TypeReference;
import org.kafkaApp.Serdes.GeneralFormat.GeneralDeserializer;
import org.kafkaApp.Structure.DataStructure;

import java.util.List;

public class ListDataStructureDeserializer extends GeneralDeserializer<List<DataStructure>> {

    public ListDataStructureDeserializer() {
        super(new TypeReference<List<DataStructure>>() {});
    }
    public ListDataStructureDeserializer(TypeReference<List<DataStructure>> type) {
        super(type);
    }

}
