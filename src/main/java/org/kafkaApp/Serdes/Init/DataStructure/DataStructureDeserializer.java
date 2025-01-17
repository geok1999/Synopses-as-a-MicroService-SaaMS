package org.kafkaApp.Serdes.Init.DataStructure;


import org.kafkaApp.Serdes.GeneralFormat.GeneralDeserializer;
import org.kafkaApp.Structure.entities.DataStructure;


public class DataStructureDeserializer extends GeneralDeserializer<DataStructure> {

    public DataStructureDeserializer() {
        super(DataStructure.class);
    }
    public DataStructureDeserializer(Class<DataStructure> type) {
        super(type);
    }
}
