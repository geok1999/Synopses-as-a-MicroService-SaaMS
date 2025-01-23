package org.kafkaApp.Microservices.SynopseMicroservice;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.metrics.sensors.ByteCountingSensor;
import org.kafkaApp.Structure.entities.DataStructure;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class TotalInputTopicTransformer implements Transformer<String, DataStructure, KeyValue<String, DataStructure>> {
    private ProcessorContext context;
    private ByteCountingSensor byteCountingSensor;
    private String sensorName;
    private String synopsesName;
   // private long totalBytes = 0;


    public TotalInputTopicTransformer(String sensorName, String synopsesName){
        this.sensorName=sensorName;
        this.synopsesName=synopsesName;
    }
    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.byteCountingSensor = ByteCountingSensor.getInstance(context, sensorName);
    }

    @Override
    public KeyValue<String, DataStructure> transform(String key, DataStructure value) {
        if (GenericSynopsesMicroService.makeMetrics) {
            byteCountingSensor.recordDataStructureType(value);
            long totalBytes = byteCountingSensor.getTotalBytes();
            //totalBytes += value.getBytes().length;
            String hostname = null;
            try {
                hostname = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }

            String output = "SynopsesName: " + synopsesName + ", VM Name: " + hostname + ", Total Bytes: " + totalBytes + "\n";
            System.out.println(output);
        /*try (FileWriter writer = new FileWriter("C:\\dataset\\MetricsResults\\ComunicationCostfile.txt", true);
             BufferedWriter bw = new BufferedWriter(writer)) {
            bw.write(output);
        } catch (IOException e) {
            e.printStackTrace();
        }*/
        }
        return new KeyValue<>(key, value);
    }

    @Override
    public void close() {
        // Any cleanup logic goes here
    }
}