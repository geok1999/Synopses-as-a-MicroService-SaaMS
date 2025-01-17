package org.kafkaApp.Serdes.Init.EstimationResult;

import org.kafkaApp.Serdes.GeneralFormat.GeneralSerde;
import org.kafkaApp.Structure.result.EstimationResult;

public class EstimationResultSerde extends GeneralSerde<EstimationResult> {

    public EstimationResultSerde() {
        super(EstimationResult.class);

    }
}
