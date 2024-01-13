package org.kafkaApp.Synopses.HyperLogLog;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

public class TestHyperLogLog {

    private static int log2m(double rsd) {
        return (int) (Math.log((1.106 / rsd) * (1.106 / rsd)) / Math.log(2));
    }

    public static void main(String[] args) {

        double rsd1=0.02;
        System.out.println(Math.log((1.106 / rsd1) * (1.106 / rsd1)));
        System.out.println(Math.log(2));
        System.out.println((Math.log((1.106 / rsd1) * (1.106 / rsd1)) / Math.log(2)));


        System.out.println(log2m(rsd1));

        String filePath = "C:\\dataset\\ForexStocks\\Forex·XAUUSD·NoExpiry.json";
        HyperLogLog hll = new HyperLogLog(rsd1); // Relative standard deviation of 1%
        Set<Double> exactSet = new HashSet<>();

        ObjectMapper objectMapper = new ObjectMapper();

        try (Stream<String> lines = Files.lines(Paths.get(filePath))) {
            lines.forEach(line -> {
                try {
                    ObjectNode node = objectMapper.readValue(line, ObjectNode.class);
                    double price = node.get("price").asDouble();
                    hll.offer(price);
                    exactSet.add(price);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            System.out.println("size of:"+hll.sizeof());
            System.out.println("Estimated number of unique prices: " + hll.cardinality());
            System.out.println("Exact number of unique prices: " + exactSet.size());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
