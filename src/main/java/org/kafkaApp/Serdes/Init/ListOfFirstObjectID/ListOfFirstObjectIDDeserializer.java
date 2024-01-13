package org.kafkaApp.Serdes.Init.ListOfFirstObjectID;


import com.fasterxml.jackson.core.type.TypeReference;
import org.kafkaApp.Serdes.GeneralFormat.GeneralDeserializer;

import java.util.List;

public class ListOfFirstObjectIDDeserializer extends GeneralDeserializer<List<String>> {


    public ListOfFirstObjectIDDeserializer(TypeReference<List<String>> type) {
        super(type);
    }
}
