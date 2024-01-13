package org.kafkaApp.Synopses.AMSSketch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serde;
import org.kafkaApp.Serdes.SynopsesSerdes.AMSSketch.AMSSketchSynopsisSerde;
import org.kafkaApp.Synopses.Synopsis;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AMSSketchSynopsis extends Synopsis {

    public AMSSketch  amss;

    public AMSSketchSynopsis(){
    }
    public AMSSketchSynopsis(String[] synopsisElements) {
        super(Integer.parseInt(synopsisElements[0]), synopsisElements[1], synopsisElements[2]);

        String [] splitParams = synopsisElements[2].split(",");
        int depth = Integer.parseInt(splitParams[0]);
        int buckets = Integer.parseInt(splitParams[1]);
        this.amss = new AMSSketch(depth,buckets);
    }


    @Override
    public Serde<? extends Synopsis> serde() {
        return new AMSSketchSynopsisSerde();
    }

    @Override
    public void add(Object obj) {
        this.amss.add((Double) obj,1);
    }

    @Override
    public Object estimate(Object obj) {
        Object []estimationParameters = (Object[]) obj;

        Object estimationVal = estimationParameters[0];
        String estimationChoice = (String) estimationParameters[1];
        if (estimationChoice.contains("L2norm")){
            return  this.amss.estimateF2();
        }

        return this.amss.estimateCount((Double) estimationVal);
    }

    @Override
    public Synopsis merge(Synopsis synopsis) {

        return null;
    }

    @Override
    public long size() {
        return this.amss.size();
    }

    public static void main(String[] args) throws IOException {
        // Create an instance of AMSSketchSynopsis with some dummy parameters
        String[] synopsisElements = {"1", "TestSynopsis", "10,10000"};
        AMSSketchSynopsis amSketchSynopsis = new AMSSketchSynopsis(synopsisElements);
        AMSSketchSynopsis amSketchSynopsis2 = new AMSSketchSynopsis(synopsisElements);
        AMSSketchSynopsis amSketchSynopsis3 = new AMSSketchSynopsis(synopsisElements);

        String filePath = "C:\\dataset\\ForexStocks\\Forex·AUDCAD·NoExpiry.json";


        ObjectMapper objectMapper = new ObjectMapper();

        Object[] estimationParamsCount1 = new Object[]{0.95272, "CountFrequency"}; //[0.1, "frequentItems"] query
        System.out.println("Estimated count for " + 0.95272 + ": " + amSketchSynopsis.estimate(estimationParamsCount1));
        try (Stream<String> lines = Files.lines(Paths.get(filePath))) {
            lines.forEach(line -> {
                try {
                    ObjectNode node = objectMapper.readValue(line, ObjectNode.class);
                    double price = node.get("price").asDouble();
                    amSketchSynopsis.add(price);
                    //amSketchSynopsis2.add(price);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
        try (Stream<String> lines = Files.lines(Paths.get(filePath))) {
            List<String> lineList = lines.collect(Collectors.toList());
            int midpoint = lineList.size() / 2;

            for (int i = 0; i < lineList.size(); i++) {
                try {
                    ObjectNode node = objectMapper.readValue(lineList.get(i), ObjectNode.class);
                    double price = node.get("price").asDouble();
                    if (i < midpoint) {
                        amSketchSynopsis2.add(price);
                    } else {
                        amSketchSynopsis3.add(price);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        double testItem = 0.95272;
        double testItem2 = 0.95257;
        double testItem3 = 0.95264;
        Object[] estimationParams = new Object[]{0, "L2norm"};
        Object[] estimationParamsCount = new Object[]{testItem, "CountFrequency"}; //[0.1, "frequentItems"] query
        Object[] estimationParamsCount2 = new Object[]{testItem2, "CountFrequency"}; //[0.1, "frequentItems"] query
        Object[] estimationParamsCount3 = new Object[]{testItem3, "CountFrequency"}; //[0.1, "frequentItems"] query

       /* System.out.println("Estimated count for " + testItem2 + ": " + amSketchSynopsis.estimate(estimationParamsCount2));

        System.out.println("Estimated count for " + testItem + ": " + amSketchSynopsis.estimate(estimationParamsCount));
          System.out.println("Estimated count for " + testItem3 + ": " + amSketchSynopsis.estimate(estimationParamsCount3));

       System.out.println("Estimated L2 norm count for sketch: " + amSketchSynopsis.estimate(estimationParams));
        System.out.println("Estimated L2 norm count for sketch: " + amSketchSynopsis2.estimate(estimationParams));
        // Print the size of the AMSSketchSynopsis instance*/
        System.out.println("Size of AMSSketchSynopsis: " + amSketchSynopsis.size());
        System.out.println("Size of AMSSketchSynopsis: " + amSketchSynopsis2.size());
        System.out.println("Size of AMSSketchSynopsis: " + amSketchSynopsis3.size());

        System.out.println("Estimated L2 norm count for sketch: " + amSketchSynopsis.estimate(estimationParams));
        System.out.println("Estimated L2 norm count for sketch: " + amSketchSynopsis2.estimate(estimationParams));
        System.out.println("Estimated L2 norm count for sketch: " + amSketchSynopsis3.estimate(estimationParams));


        // AMSSketchSynopsis newAms= (AMSSketchSynopsis) amSketchSynopsis.merge(amSketchSynopsis2);
       // System.out.println("Size of AMSSketchSynopsis: " + newAms.size());
    }
}
