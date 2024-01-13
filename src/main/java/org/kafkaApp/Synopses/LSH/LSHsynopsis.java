package org.kafkaApp.Synopses.LSH;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.kafka.common.serialization.Serde;
import org.kafkaApp.Serdes.SynopsesSerdes.LSH.LSHSynopsisSerde;
import org.kafkaApp.Synopses.Synopsis;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class LSHsynopsis extends Synopsis {
    public LSH lsh;
    private NormalDistribution nd;
    public LSHsynopsis(){}
    public LSHsynopsis(String[] synopsisElements) {
        super(Integer.parseInt(synopsisElements[0]), synopsisElements[1], synopsisElements[2]);
        String[] splitParams = synopsisElements[2].split(",");

        nd = new NormalDistribution(0, 1);
        lsh = new LSH("",splitParams);

    }

    @Override
    public Serde<? extends Synopsis> serde() {
        return new LSHSynopsisSerde();
    }

    @Override
    public void add(Object k) {
        lsh.add((Double) k);
    }


    @Override
    public Object estimate(Object k) {
        String[] splitParams = this.getSynopsisParameters().split(",");
        int W = Integer.parseInt(splitParams[0]);
        int D = Integer.parseInt(splitParams[1]);
        int B = Integer.parseInt(splitParams[2]);
        double[][] g = new double[W][D];


        for (int r=0;r<W;r++){
            for (int c=0;c<D;c++) {
                g[r][c] = nd.sample();
            }
        }
        BitSet bitSet=lsh.estimate(g);
        if(bitSet==null)
            return null;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bitSet.length(); i++) {
            if (bitSet.get(i)) {
                sb.append('1');
            } else {
                sb.append('0');
            }
        }
        return sb.toString();
    }

    @Override
    public Synopsis merge(Synopsis sk) {
        // TODO: Implement if needed
        return null;
    }



    @Override
    public long size() {
        return 0;
    }
    public static void main(String[] args) throws IOException {
        // Define the synopsis elements
        String[] synopsisElements = new String[]{
                "1",             // synopsisID
                "LSHsynopsis",   // synopsisType
                "100,100,20"        // params: W, D, B
        };

        // Create an LSHsynopsis object
        LSHsynopsis lshSynopsis = new LSHsynopsis(synopsisElements);

        String filePath = "C:\\dataset\\ForexStocks\\Forex·AUDCAD·NoExpiry.json";


        ObjectMapper objectMapper = new ObjectMapper();
        AtomicInteger i= new AtomicInteger();
        try (Stream<String> lines = Files.lines(Paths.get(filePath))) {
            lines.forEach(line -> {
                try {
                    ObjectNode node = objectMapper.readValue(line, ObjectNode.class);
                    double price = node.get("price").asDouble();
                    lshSynopsis.add(price);
                    i.getAndIncrement();

                    if((i.get() >999 && i.get() %1000==0) || i.get()==50){
                        Object estimate = lshSynopsis.estimate(null);
                       // System.out.println("Estimated value: " +i+", "+ estimate);

                        System.out.println("Signatures after all entries: " +i+", "+  estimate);

                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }


      /*  // Add some data to the LSHsynopsis
        for (int i = 0; i < 100000; i++) {
            lshSynopsis.add((double) i);
            if(i>999 && i%1000==0){
                Object estimate = lshSynopsis.estimate(null);
                System.out.println("Estimated value: " +i+", "+ estimate);

                System.out.println("Signatures after all entries: " +i+", "+  bitSetToString((BitSet) estimate));

            }

        }

        // Estimate a value
        Object estimate = lshSynopsis.estimate(null);

        // Print the estimated value
        System.out.println("Estimated value: " + estimate);

        System.out.println("Signatures after all entries: " + bitSetToString((BitSet) estimate));
*/
    }


    /*public static void main(String[] args) throws IOException {
        String[] synopsisElements = new String[]{
                "4",             // synopsisID
                "EURTRY,Forex,4,price",      // synopsisType
                "1000,10,20" // params: W, D, B (increased D to 8)
        };

        LSHsynopsis lshSynopsis = new LSHsynopsis(synopsisElements);

        // Read from the JSON file and add the price field
        ObjectMapper objectMapper = new ObjectMapper();
        File jsonFile = new File("C:\\dataset\\ForexStocks\\Forex·XAUUSD·NoExpiry.json");
        if(jsonFile.exists() && !jsonFile.isDirectory()) {
            JsonNode rootNode = objectMapper.readTree(jsonFile);

            // Assuming the rootNode is an array of JSON objects
            for (JsonNode node : rootNode) {
                if (node.has("price")) {
                    double price = node.get("price").asDouble();
                    lshSynopsis.add(price);
                }
            }

            // If there are remaining entries less than a batch
            BitSet result = (BitSet) lshSynopsis.estimate(null);
            System.out.println("Signatures after all entries: " + lshSynopsis.bitSetToString(result));
        } else {
            System.out.println("File does not exist or is a directory");
        }
    }*/
}
