package org.kafkaApp.Synopses.BloomFilter;

import com.clearspring.analytics.stream.membership.BloomFilter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class TestBloomFilter {
    public static void main(String[] args) {
        String filePath = "C:\\dataset\\ForexStocks\\Forex·XAUUSD·NoExpiry.json";
        BloomFilter bloomFilter = new BloomFilter(150000, 0.01); // numElements and maxFalsePosProbability


        ObjectMapper objectMapper = new ObjectMapper();

        try (Stream<String> lines = Files.lines(Paths.get(filePath))) {
            lines.forEach(line -> {
                try {
                    ObjectNode node = objectMapper.readValue(line, ObjectNode.class);
                    double price = node.get("price").asDouble();
                    bloomFilter.add(String.valueOf(price));

                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            System.out.println("Estimate for '" + 1282.52 + "': " + bloomFilter.isPresent(String.valueOf(1282.52)));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
