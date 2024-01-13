package org.kafkaApp.Serdes.Init.RequestWithDataStructure;


import com.fasterxml.jackson.core.type.TypeReference;
import org.kafkaApp.Serdes.GeneralFormat.GeneralSerde;
import org.kafkaApp.Structure.RequestWithDataStructure;



public class RequestWithDataStructureSerde extends GeneralSerde<RequestWithDataStructure> {


	public RequestWithDataStructureSerde() {
		super(new TypeReference<RequestWithDataStructure>() {});
	}

}
