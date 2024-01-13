package org.kafkaApp.Serdes.Init.DataWithMetaDataParameters;


import com.fasterxml.jackson.core.type.TypeReference;
import org.kafkaApp.Serdes.GeneralFormat.GeneralSerde;
import org.kafkaApp.Structure.DataStructure;
import org.kafkaApp.Structure.StructureWithMetaDataParameters;

public class DataWithMetaDataParametersSerde extends GeneralSerde<StructureWithMetaDataParameters<DataStructure>> {

	public DataWithMetaDataParametersSerde() {
		super(new TypeReference<StructureWithMetaDataParameters<DataStructure>>() {});
		// TODO Auto-generated constructor stub
	}

}
