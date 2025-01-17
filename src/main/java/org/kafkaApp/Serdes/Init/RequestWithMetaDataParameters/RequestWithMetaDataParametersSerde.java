package org.kafkaApp.Serdes.Init.RequestWithMetaDataParameters;


import com.fasterxml.jackson.core.type.TypeReference;
import org.kafkaApp.Serdes.GeneralFormat.GeneralSerde;
import org.kafkaApp.Structure.entities.RequestStructure;
import org.kafkaApp.Structure.dto.StructureWithMetaDataParameters;

public class RequestWithMetaDataParametersSerde extends GeneralSerde<StructureWithMetaDataParameters<RequestStructure>> {

	public RequestWithMetaDataParametersSerde() {
		super(new TypeReference<StructureWithMetaDataParameters<RequestStructure>>() {});
		// TODO Auto-generated constructor stub
	}

}
