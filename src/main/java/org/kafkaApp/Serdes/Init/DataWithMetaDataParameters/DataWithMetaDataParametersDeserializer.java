package org.kafkaApp.Serdes.Init.DataWithMetaDataParameters;


import org.kafkaApp.Serdes.GeneralFormat.GeneralDeserializer;
import org.kafkaApp.Structure.entities.DataStructure;
import org.kafkaApp.Structure.dto.StructureWithMetaDataParameters;

public class DataWithMetaDataParametersDeserializer extends GeneralDeserializer<StructureWithMetaDataParameters<DataStructure>> {


    public DataWithMetaDataParametersDeserializer(Class<StructureWithMetaDataParameters<DataStructure>> type) {
        super(type);
    }
}
