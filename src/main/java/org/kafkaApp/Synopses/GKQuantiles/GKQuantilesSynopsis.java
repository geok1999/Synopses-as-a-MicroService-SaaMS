package org.kafkaApp.Synopses.GKQuantiles;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serde;
import org.kafkaApp.Synopses.Synopsis;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class GKQuantilesSynopsis extends Synopsis {
   public GKQuantiles gkq;
    public GKQuantilesSynopsis(String[] synopsisElements) {
        super(Integer.parseInt(synopsisElements[0]), synopsisElements[1], synopsisElements[2]);

        String [] splitParams = synopsisElements[2].split(",");
        double epsilon = Double.parseDouble(splitParams[0]);
        this.gkq = new GKQuantiles(epsilon);
    }
    @Override
    public Serde<? extends Synopsis> serde() {
        return null;
    }

    @Override
    public void add(Object obj) {
        gkq.offer((Double) obj);
    }

    @Override
    public Object estimate(Object obj) {
        return gkq.getQuantile((Double) obj);
    }

    @Override
    public Synopsis merge(Synopsis synopsis) {
        return null;
    }

    @Override
    public long size() {
        return 0;
    }
    public static void main(String[] args) throws IOException {
        // Create the first GKQuantilesSynopsis
        String[] elements1 = {"1", "GKQuantilesSynopsis1", "0.01"};
        GKQuantilesSynopsis synopsis1 = new GKQuantilesSynopsis(elements1);

        String filePath = "C:\\dataset\\ForexStocks\\Forex·AUDCAD·NoExpiry.json";


        ObjectMapper objectMapper = new ObjectMapper();

        try (Stream<String> lines = Files.lines(Paths.get(filePath))) {
            lines.forEach(line -> {
                try {
                    ObjectNode node = objectMapper.readValue(line, ObjectNode.class);
                    double price = node.get("price").asDouble();
                    synopsis1.add(price);
                    synopsis1.add(price);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }


        // Estimate the 50th percentile (median) for both synopses
        Double quantile1 = (Double) synopsis1.estimate(0.5);

        // Print the results
        System.out.println("50th percentile for synopsis1: " + quantile1);


        // Note: Merging is not implemented in the provided GKQuantiles class.
        // If you had a merge method, you could merge the two synopses and estimate quantiles for the merged data.
    }

}
