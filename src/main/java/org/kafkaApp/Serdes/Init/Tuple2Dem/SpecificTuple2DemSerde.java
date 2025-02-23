package org.kafkaApp.Serdes.Init.Tuple2Dem;

import com.fasterxml.jackson.core.type.TypeReference;
import org.kafkaApp.Structure.entities.DataStructure;
import org.kafkaApp.Structure.entities.RequestStructure;
import org.kafkaApp.Structure.dto.StructureWithMetaDataParameters;


public class SpecificTuple2DemSerde extends Tuple2DemSerde<DataStructure, StructureWithMetaDataParameters<RequestStructure>> {
    public SpecificTuple2DemSerde() {
        super(new TypeReference<DataStructure>() {}, new TypeReference<StructureWithMetaDataParameters<RequestStructure>>() {});
    }
}
