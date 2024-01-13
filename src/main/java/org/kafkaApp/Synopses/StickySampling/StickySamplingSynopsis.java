package org.kafkaApp.Synopses.StickySampling;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serde;
import org.kafkaApp.Serdes.SynopsesSerdes.StickySampling.StickySamplingSynopsisSerdes;
import org.kafkaApp.Synopses.Synopsis;
import org.streaminer.stream.frequency.util.CountEntry;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

public class StickySamplingSynopsis extends Synopsis {

    public StickySampling<Object> stickySamplings;
    public StickySamplingSynopsis(){
    }
    public StickySamplingSynopsis(String[] synopsisElements) {
        super(Integer.parseInt(synopsisElements[0]), synopsisElements[1], synopsisElements[2]);

        String [] splitParams = synopsisElements[2].split(",");
        double support  = Double.parseDouble(splitParams[0]);
        double error = Double.parseDouble(splitParams[1]);
        double probabilityOfFailure = Double.parseDouble(splitParams[2]);
        this.stickySamplings = new StickySampling<>(support,error,probabilityOfFailure);
    }

    @Override
    public Serde<? extends Synopsis> serde() {
        return new StickySamplingSynopsisSerdes();
    }

    @Override
    public void add(Object obj) {
        stickySamplings.add((Double) obj, 1);
    }

    @Override
    public Object estimate(Object obj) {
        Object []estimationParameters = (Object[]) obj;
        Object estimationVal = estimationParameters[0];
        String estimationChoice = (String) estimationParameters[1];
        if (estimationChoice.contains("FrequentItems")){
            double support = (double) estimationVal;
            return this.stickySamplings.getFrequentItems(support);
        }
        else if(estimationChoice.contains("isFrequent")){
            Integer estimationValInteger = (Integer) estimationParameters[0];
            long estimationValLong = estimationValInteger.longValue();
            return this.stickySamplings.isFrequent(estimationValLong);
        }
        return this.stickySamplings.estimateCount(estimationVal);
    }

    @Override
    public Synopsis merge(Synopsis synopsis) {
        return null;
    }

    @Override
    public long size() {
        return stickySamplings.size();
    }

    public static void main(String[] args) throws IOException {
        // Create an instance of StickySamplingSynopsis with some dummy parameters
        String[] synopsisElements = {"1", "TestSynopsis", "0.001,0.0001,0.001"};
        StickySamplingSynopsis stickySamplingSynopsis = new StickySamplingSynopsis(synopsisElements);

        String filePath = "C:\\dataset\\ForexStocks\\Forex·AUDCAD·NoExpiry.json";


        ObjectMapper objectMapper = new ObjectMapper();

        try (Stream<String> lines = Files.lines(Paths.get(filePath))) {
            lines.forEach(line -> {
                try {
                    ObjectNode node = objectMapper.readValue(line, ObjectNode.class);
                    double price = node.get("price").asDouble();
                    stickySamplingSynopsis.add(price);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }

        double testItemString =0.95272;
        double testItem = 0.01;
        long frequency = 5;
        Object[] estimationParams = new Object[]{testItem, "frequentItems"}; //[0.1, "frequentItems"] query
        Object[] estimationParamsisFrequent = new Object[]{frequency, "isFrequent"}; //[0.1, "frequentItems"] query
        Object[] estimationParamsCount = new Object[]{testItemString, "CountItems"}; //[0.1, "frequentItems"] query
        System.out.println("Estimated count for " + testItemString + ": " + stickySamplingSynopsis.estimate(estimationParamsCount));

        boolean isFrequent = (boolean) stickySamplingSynopsis.estimate(estimationParamsisFrequent);
        System.out.println("The frequency " + frequency + " is considered frequent: " + isFrequent);


        List<CountEntry<Object>> frequentItems = (List<CountEntry<Object>>) stickySamplingSynopsis.estimate(estimationParams);

        System.out.println("\nFrequent Items with Minimum Support of 0.1:");
        for (CountEntry<Object> entry : frequentItems) {
            System.out.println("Item: " + entry.item + ", Frequency: " + entry.frequency);
        }

        // Print the size of the Lossy Counting structure
        System.out.println("\nSize of Stricky Sampling Structure: " + stickySamplingSynopsis.size());
    }
}
