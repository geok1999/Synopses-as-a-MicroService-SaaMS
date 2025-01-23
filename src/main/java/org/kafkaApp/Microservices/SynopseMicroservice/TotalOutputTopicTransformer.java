package org.kafkaApp.Microservices.SynopseMicroservice;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.metrics.sensors.ByteCountingSensor;
import org.kafkaApp.Structure.dto.SynopsisAndParameters;

public class TotalOutputTopicTransformer implements Transformer<String, SynopsisAndParameters, KeyValue<String, SynopsisAndParameters>> {
    private ProcessorContext context;
    private ByteCountingSensor byteCountingSensor;
    private String sensorName;
    private String synopsesName;
   // private long totalBytes = 0;


    public TotalOutputTopicTransformer(String sensorName,String synopsesName){
        this.sensorName=sensorName;
        this.synopsesName=synopsesName;
    }
    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.byteCountingSensor = ByteCountingSensor.getInstance(context, sensorName);
    }

    @Override
    public KeyValue<String, SynopsisAndParameters> transform(String key, SynopsisAndParameters value) {
        if (GenericSynopsesMicroService.makeMetrics) {
            byteCountingSensor.record(value);
            long totalBytes = byteCountingSensor.getTotalBytes();
            //totalBytes += value.getBytes().length;
            String hostname = "NewVM";
       /* try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }*/

            String output = "SynopsesName: " + synopsesName+ ", VM Name: " + hostname + ", Total Bytes: " + totalBytes + "\n";
            System.out.println(output);
        /*try (FileWriter writer = new FileWriter("C:\\dataset\\MetricsResults\\ComunicationCostfile.txt", true);
             BufferedWriter bw = new BufferedWriter(writer)) {
            bw.write(output);
        } catch (IOException e) {
            e.printStackTrace();
        }*/
            return new KeyValue<>(key, value);
        }
        return null;
    }

    @Override
    public void close() {
        // Any cleanup logic goes here
    }
}