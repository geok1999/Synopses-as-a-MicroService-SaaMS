package org.kafkaApp.Synopses;

import com.clearspring.analytics.stream.frequency.CountMinSketch;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.kafkaApp.Serdes.SynopsesSerdes.CountMin.CountMinSerde;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;


public class CountMin extends Synopsis{

    public CountMinSketch cm;
    public CountMin(){
    }
    public CountMin(String[] synopsisElements) {
        super(Integer.parseInt(synopsisElements[0]), synopsisElements[1], synopsisElements[2]);

        String [] splitParams = synopsisElements[2].split(",");
        double epsOfTotalCount  = Double.parseDouble(splitParams[0]);
        double confidence = Double.parseDouble(splitParams[1]);
        int seed = Integer.parseInt(splitParams[2]);
        this.cm = new CountMinSketch(epsOfTotalCount,confidence,seed);
    }

    @Override
    public Serde<CountMin> serde() {
        return new CountMinSerde();
    }

    @Override
    public void add(Object obj) {
        // TODO Auto-generated method stub
        this.cm.add(obj.toString(), 1);
    }

    @Override
    public Object estimate(Object obj) {
        // TODO Auto-generated method stub
        return this.cm.estimateCount(obj.toString());

    }

    @Override
    public Synopsis merge(Synopsis synopsis) {
        if (!(synopsis instanceof CountMin)) {
            throw new IllegalArgumentException("It's not the required synopsis");
        }
        CountMin otherCountMin = (CountMin) synopsis;
        CountMinSketch mergedCM;
        try {
            mergedCM = CountMinSketch.merge(this.cm, otherCountMin.cm);
         //   System.out.println("size of merged synopsis"+mergedCM.size());
        } catch (Exception e) {
            throw new RuntimeException("Error when merging CountMinSketch instances.", e);
        }
        CountMin mergedCountMin = new CountMin();
        mergedCountMin.cm = mergedCM;
       // System.out.println("size of merged synopsis"+ mergedCountMin.cm.size());
        return mergedCountMin;
    }

    @Override
    public long size() {
        return cm.size();
    }

    public void saveCountMin(String filename) {
        try (CountMinSerde countMinSerde = new CountMinSerde();
             FileOutputStream fileOut = new FileOutputStream(filename);
             DataOutputStream dataOut = new DataOutputStream(fileOut)) {

            Serializer<CountMin> serializer = countMinSerde.serializer();
            byte[] serializedData = serializer.serialize("", this);
            dataOut.write(serializedData);
            System.out.println("CountMin object has been written to " + filename);
        } catch (IOException e) {
            // Log the error or re-throw it as a runtime exception
            throw new RuntimeException("Error writing CountMin object to disk", e);
        }
    }


    public static void main(String[] args) {
            String[] synopsisElements = new String[]{
                    "1",             // synopsisID
                    "CountMin",      // synopsisType
                    "params,help,help,0.01,0.99,123456" // params,epsOfTotalCount,confidence,seed
            };

            // Instantiate CountMin class for countMin1
            CountMin countMin1 = new CountMin(synopsisElements);

            // Add elements to the CountMinSketch of countMin1
            countMin1.add("apple");
            countMin1.add("banana");
            countMin1.add("orange");
            countMin1.add("apple");

            // Instantiate CountMin class for countMin2
            CountMin countMin2 = new CountMin(synopsisElements);

            // Add elements to the CountMinSketch of countMin2
            countMin2.add("apple");
            countMin2.add("banana");
            countMin2.add("orange");

            // Merge countMin1 and countMin2
            CountMin mergedCountMin = (CountMin) countMin1.merge(countMin2);

            // Estimate the frequency of an element in the merged CountMin
            long appleFrequency = (long) mergedCountMin.estimate("apple");
            System.out.println("Frequency of 'apple': " + appleFrequency);

            long bananaFrequency = (long) mergedCountMin.estimate("banana");
            System.out.println("Frequency of 'banana': " + bananaFrequency);

            long orangeFrequency = (long) mergedCountMin.estimate("orange");
            System.out.println("Frequency of 'orange': " + orangeFrequency);
        }

}
