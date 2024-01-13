package org.kafkaApp.Synopses.DFT;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class TestDFT {
    public static void main(String[] args) {
        String filePath = "C:\\dataset\\ForexStocks\\Forex·EURTRY·NoExpiry.json";
        String[] synopsisElements = new String[]{
                "4",             // synopsisID
                "EURTRY,Forex,4,price",      // synopsisType
                "1,500,2000,8" // parameters for DFTSynopsis
        };
        DFTSynopsis dftSynopsis = new DFTSynopsis(synopsisElements);

        ObjectMapper objectMapper = new ObjectMapper();

        try (Stream<String> lines = Files.lines(Paths.get(filePath))) {
            final int[] count = {0};
            lines.forEach(line -> {
                try {
                    ObjectNode node = objectMapper.readValue(line, ObjectNode.class);
                    double price = node.get("price").asDouble();
                    dftSynopsis.add(price);
                    count[0]++;
                    if (count[0] % 2000 == 0) {
                        System.out.println("Estimated Fourier Coefficients after " + count[0] + " data points:");
                        System.out.println(dftSynopsis.estimate(null));
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            System.out.println("Final Estimated Fourier Coefficients:");
            System.out.println(dftSynopsis.estimate(null));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
