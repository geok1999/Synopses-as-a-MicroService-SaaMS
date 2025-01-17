package org.kafkaApp.Serdes.Init.Tuple2Dem;

import com.fasterxml.jackson.core.type.TypeReference;
import org.kafkaApp.Serdes.GeneralFormat.GeneralSerde;
import org.kafkaApp.Structure.entities.RequestStructure;
import org.kafkaApp.Structure.dto.SynopsisAndParameters;
import org.kafkaApp.Structure.dto.Tuple2Dem;

public class KTableMergejoinTuple2DemSerde extends GeneralSerde<Tuple2Dem<RequestStructure, SynopsisAndParameters>> {

    public KTableMergejoinTuple2DemSerde() {
        super(new TypeReference<>() {
        });
    }
}