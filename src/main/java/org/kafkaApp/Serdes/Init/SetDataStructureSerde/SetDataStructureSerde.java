package org.kafkaApp.Serdes.Init.SetDataStructureSerde;

import com.fasterxml.jackson.core.type.TypeReference;
import org.kafkaApp.Serdes.GeneralFormat.GeneralSerde;
import org.kafkaApp.Structure.entities.DataStructure;

import java.util.Set;

public class SetDataStructureSerde extends GeneralSerde<Set<DataStructure>> {

    public SetDataStructureSerde() {
        super(new TypeReference<Set<DataStructure>>() {});
        // TODO Auto-generated constructor stub
    }

}
