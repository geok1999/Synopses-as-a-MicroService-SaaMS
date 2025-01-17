package org.kafkaApp.Serdes.Init.Tuple2Dem;

import com.fasterxml.jackson.core.type.TypeReference;
import org.kafkaApp.Serdes.GeneralFormat.GeneralSerde;
import org.kafkaApp.Structure.dto.SynopsisAndParameters;
import org.kafkaApp.Structure.dto.Tuple2Dem;
import org.kafkaApp.Synopses.Synopsis;


public class LoadTupleTuple2demSerde extends GeneralSerde<Tuple2Dem<SynopsisAndParameters ,Synopsis>> {

    public LoadTupleTuple2demSerde() {
        super(new TypeReference<>() {
        });
    }
}