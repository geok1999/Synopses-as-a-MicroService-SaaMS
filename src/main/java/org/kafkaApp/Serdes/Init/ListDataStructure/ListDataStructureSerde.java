package org.kafkaApp.Serdes.Init.ListDataStructure;


import com.fasterxml.jackson.core.type.TypeReference;
import org.kafkaApp.Serdes.GeneralFormat.GeneralSerde;
import org.kafkaApp.Structure.DataStructure;

import java.util.List;

public class ListDataStructureSerde extends GeneralSerde<List<DataStructure>> {

	public ListDataStructureSerde() {
		super(new TypeReference<List<DataStructure>>() {});
		// TODO Auto-generated constructor stub
	}

}
