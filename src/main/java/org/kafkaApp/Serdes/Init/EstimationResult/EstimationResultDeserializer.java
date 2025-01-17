package org.kafkaApp.Serdes.Init.EstimationResult;


import org.kafkaApp.Serdes.GeneralFormat.GeneralDeserializer;
import org.kafkaApp.Structure.result.EstimationResult;


public class EstimationResultDeserializer extends GeneralDeserializer<EstimationResult> {

    public EstimationResultDeserializer() {
        super(EstimationResult.class);
    }
    public EstimationResultDeserializer(Class<EstimationResult> type) {
        super(type);
    }
}
