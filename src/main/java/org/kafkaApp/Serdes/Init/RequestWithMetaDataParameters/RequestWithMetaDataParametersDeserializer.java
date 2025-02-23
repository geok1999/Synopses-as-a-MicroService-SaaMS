package org.kafkaApp.Serdes.Init.RequestWithMetaDataParameters;


import org.kafkaApp.Serdes.GeneralFormat.GeneralDeserializer;
import org.kafkaApp.Structure.entities.RequestStructure;
import org.kafkaApp.Structure.dto.StructureWithMetaDataParameters;

public class RequestWithMetaDataParametersDeserializer extends GeneralDeserializer<StructureWithMetaDataParameters<RequestStructure>> {


    public RequestWithMetaDataParametersDeserializer(Class<StructureWithMetaDataParameters<RequestStructure>> type) {
        super(type);
    }
}
