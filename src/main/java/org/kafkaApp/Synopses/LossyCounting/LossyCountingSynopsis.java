package org.kafkaApp.Synopses.LossyCounting;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serde;
import org.kafkaApp.Serdes.SynopsesSerdes.LossyCounting.LossyCountingSynopsisSerdes;
import org.kafkaApp.Synopses.Synopsis;
import org.streaminer.stream.frequency.util.CountEntry;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;


public class LossyCountingSynopsis extends Synopsis {

    public LossyCounting<Object> sk;

    public LossyCountingSynopsis(){
    }
    public LossyCountingSynopsis(String[] synopsisElements) {
        super(Integer.parseInt(synopsisElements[0]), synopsisElements[1], synopsisElements[2]);

        String [] splitParams = synopsisElements[2].split(",");
        double maxError = Double.parseDouble(splitParams[0]);
        this.sk = new LossyCounting<>(maxError);
    }


    @Override
    public Serde<? extends Synopsis> serde() {
        return new LossyCountingSynopsisSerdes();
    }

    @Override
    public void add(Object obj) {
        this.sk.add( obj, 1);
    }

    @Override
    public Object estimate(Object obj) {
        Object []estimationParameters = (Object[]) obj;

        Object estimationVal = estimationParameters[0];
        String estimationChoice = (String) estimationParameters[1];
        if (estimationChoice.contains("FrequentItems")){
            double support = (double) estimationVal;
            return this.sk.getFrequentItems(support);
        }


        return this.sk.estimateCount(estimationVal);
    }

    @Override
    public Synopsis merge(Synopsis synopsis) {
        return null;
    }

    @Override
    public long size() {
        return this.sk.size();
    }

    public static void main(String[] args) throws IOException {

        String[] synopsisElements = new String[]{
                "1",             // synopsisID
                "CountMin",      // synopsisType
                "0.001" // params,epsOfTotalCount,confidence,seed
        };
        LossyCountingSynopsis lcSynopsis = new LossyCountingSynopsis(synopsisElements);

        String filePath = "C:\\dataset\\ForexStocks\\Forex·AUDCAD·NoExpiry.json";


        ObjectMapper objectMapper = new ObjectMapper();

        try (Stream<String> lines = Files.lines(Paths.get(filePath))) {
            lines.forEach(line -> {
                try {
                    ObjectNode node = objectMapper.readValue(line, ObjectNode.class);
                    double price = node.get("price").asDouble();
                    lcSynopsis.add(price);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }

        // Estimate count for a specific item
        double testItemString =0.95272;
        double testItem = 0.001;
        Object[] estimationParams = new Object[]{testItem, "frequentItems"}; //[0.1, "frequentItems"] query
        Object[] estimationParamsCount = new Object[]{testItemString, "CountItems"}; //[0.1, "frequentItems"] query
        System.out.println("Estimated count for " + testItemString + ": " + lcSynopsis.estimate(estimationParamsCount));

        // Print frequent items with a minimum support of 0.1 (adjust as needed)
        List<CountEntry<Object>> frequentItems = (List<CountEntry<Object>>) lcSynopsis.estimate(estimationParams);

        System.out.println("\nFrequent Items with Minimum Support of 0.1:");
        for (CountEntry<Object> entry : frequentItems) {
            System.out.println("Item: " + entry.item + ", Frequency: " + entry.frequency);
        }

        // Print the size of the Lossy Counting structure
        System.out.println("\nSize of Lossy Counting Structure: " + lcSynopsis.size());
    }
}
