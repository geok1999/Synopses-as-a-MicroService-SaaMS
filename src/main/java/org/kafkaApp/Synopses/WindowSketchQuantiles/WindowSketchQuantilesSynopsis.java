package org.kafkaApp.Synopses.WindowSketchQuantiles;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serde;
import org.kafkaApp.Serdes.SynopsesSerdes.WindowSketchQuantiles.WindowSketchQuantilesSynopsisSerdes;
import org.kafkaApp.Synopses.Synopsis;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class WindowSketchQuantilesSynopsis extends Synopsis {
   public WindowSketchQuantiles wsq;
    public WindowSketchQuantilesSynopsis(String[] synopsisElements) {
        super(Integer.parseInt(synopsisElements[0]), synopsisElements[1], synopsisElements[2]);

        String [] splitParams = synopsisElements[2].split(",");
        double epsilon = Double.parseDouble(splitParams[0]);
        this.wsq = new WindowSketchQuantiles(epsilon,Integer.parseInt(splitParams[1]));


    }
    @Override
    public Serde<? extends Synopsis> serde() {
        return new WindowSketchQuantilesSynopsisSerdes();
    }

    @Override
    public void add(Object obj) {
        wsq.offer((Double) obj);
    }

    @Override
    public Object estimate(Object obj) {

        return wsq.getQuantile((Double) obj);

    }

    @Override
    public Synopsis merge(Synopsis synopsis) {
        return null;
    }

    @Override
    public long size() {
        return wsq.getElementCount();
    }

    public static void main(String[] args) throws IOException {
        // Create the first GKQuantilesSynopsis
        String[] elements1 = {"1", "WindowSketchQuantiles", "0.01,20000"};
        WindowSketchQuantilesSynopsis synopsis1 = new WindowSketchQuantilesSynopsis(elements1);


        String filePath = "C:\\dataset\\ForexStocks\\Forex·AUDCAD·NoExpiry.json";
        ObjectMapper objectMapper = new ObjectMapper();
        AtomicInteger i = new AtomicInteger();
        try (Stream<String> lines = Files.lines(Paths.get(filePath))) {
            lines.forEach(line -> {
                try {
                    ObjectNode node = null;

                    node = objectMapper.readValue(line, ObjectNode.class);

                    double price = node.get("price").asDouble();
                    synopsis1.add(price);
                    i.getAndIncrement();
                   // System.out.println(i);
                    if (i.get() % 53852 == 0 && i.get() != 0) {
                        System.out.println("Size "+synopsis1.size());
                        Double quantile1 ;
                        quantile1=(Double) synopsis1.estimate(0.5);
                        System.out.println("50th percentile for synopsis1 after " + i + " elements: " + quantile1);

                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }

            });
            Double quantile1 ;
            // Final estimation after all elements have been added
            quantile1= (Double) synopsis1.estimate(0.5);
            System.out.println("50th percentile for synopsis1 after all elements: " + quantile1);
        }
    }
       /* String filePath = "C:\\dataset\\ForexStocks\\Forex·AUDCAD·NoExpiry.json";


        ObjectMapper objectMapper = new ObjectMapper();
        AtomicInteger i= new AtomicInteger();
        try (Stream<String> lines = Files.lines(Paths.get(filePath))) {
            lines.forEach(line -> {
                try {
                    ObjectNode node = objectMapper.readValue(line, ObjectNode.class);
                    double price = node.get("price").asDouble();
                    synopsis1.add(price);
                    i.getAndIncrement();
                    if((i.get() >32767 && i.get() %32768==0) ){
                        Double quantile1 = (Double) synopsis1.estimate(0.5);
                        System.out.println("50th percentile for synopsis1: "+i+", " + quantile1);
                    }

                    //synopsis1.add(price);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }


        // Estimate the 50th percentile (median) for both synopses


*/
        // Note: Merging is not implemented in the provided GKQuantiles class.
        // If you had a merge method, you could merge the two synopses and estimate quantiles for the merged data.


    }
