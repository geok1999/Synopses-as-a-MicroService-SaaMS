package org.kafkaApp.Serdes.Init.ListOfFirstObjectID;


import com.fasterxml.jackson.core.type.TypeReference;
import org.kafkaApp.Serdes.GeneralFormat.GeneralSerde;

import java.util.List;

public class ListOfFirstObjectIDSerde extends GeneralSerde<List<String>> {

	public ListOfFirstObjectIDSerde() {
		super(new TypeReference<List<String>>() {});
		// TODO Auto-generated constructor stub
	}

}
