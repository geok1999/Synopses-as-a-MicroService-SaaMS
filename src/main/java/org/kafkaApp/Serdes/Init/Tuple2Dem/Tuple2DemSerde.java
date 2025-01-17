package org.kafkaApp.Serdes.Init.Tuple2Dem;

import com.fasterxml.jackson.core.type.TypeReference;
import org.kafkaApp.Serdes.GeneralFormat.GeneralSerde;
import org.kafkaApp.Structure.dto.Tuple2Dem;

public class Tuple2DemSerde<A, B> extends GeneralSerde<Tuple2Dem<A, B>> {

    public Tuple2DemSerde(Class<A> classA, Class<B> classB) {
        super(new TypeReference<Tuple2Dem<A, B>>() {});
    }
    public Tuple2DemSerde() {
        super(new TypeReference<Tuple2Dem<A, B>>() {});
    }
    public Tuple2DemSerde(TypeReference<A> typeA, TypeReference<B> typeB) {
        super(new TypeReference<Tuple2Dem<A, B>>() {});
    }
    public Tuple2DemSerde(TypeReference<Tuple2Dem<A, B>> type) {
        super(type);
    }
}
