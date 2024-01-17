package org.kafkaApp.Serdes.Init.Tuple2Dem;

import com.fasterxml.jackson.core.type.TypeReference;
import org.kafkaApp.Serdes.GeneralFormat.GeneralSerde;
import org.kafkaApp.Structure.RequestStructure;
import org.kafkaApp.Structure.SynopsisAndParameters;
import org.kafkaApp.Structure.Tuple2Dem;
import org.kafkaApp.Synopses.Synopsis;

public class KTableMergejoinTuple2DemSerde extends GeneralSerde<Tuple2Dem<RequestStructure, SynopsisAndParameters>> {

    public KTableMergejoinTuple2DemSerde() {
        super(new TypeReference<>() {
        });
    }
}