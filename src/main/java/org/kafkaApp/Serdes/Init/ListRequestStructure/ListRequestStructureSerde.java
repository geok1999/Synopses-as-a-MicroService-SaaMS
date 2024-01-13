package org.kafkaApp.Serdes.Init.ListRequestStructure;


import com.fasterxml.jackson.core.type.TypeReference;
import org.kafkaApp.Serdes.GeneralFormat.GeneralSerde;
import org.kafkaApp.Structure.RequestStructure;

import java.util.List;

public class ListRequestStructureSerde extends GeneralSerde<List<RequestStructure>> {

	public ListRequestStructureSerde() {
		super(new TypeReference<List<RequestStructure>>() {});
		// TODO Auto-generated constructor stub
	}

}
