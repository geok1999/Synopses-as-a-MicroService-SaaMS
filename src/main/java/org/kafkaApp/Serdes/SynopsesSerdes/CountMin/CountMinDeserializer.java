package org.kafkaApp.Serdes.SynopsesSerdes.CountMin;

import com.clearspring.analytics.stream.frequency.CountMinSketch;
import org.apache.kafka.common.serialization.Deserializer;
import org.kafkaApp.Synopses.CountMin;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class CountMinDeserializer implements Deserializer<CountMin> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Implement if needed
    }

    @Override
    public CountMin deserialize(String topic, byte[] data) {
        try (ByteArrayInputStream byteStream = new ByteArrayInputStream(data);
               GZIPInputStream gzipStream = new GZIPInputStream(byteStream);
               DataInputStream dis = new DataInputStream(gzipStream)) {
            String[] synopsisInfo = new String[3];
            // First read the synopsis fields
            int synopsesID = dis.readInt();  // reading int
            synopsisInfo[0] = Integer.toString(synopsesID);

            String synopsisDetails = dis.readUTF();  // reading String
            synopsisInfo[1] = synopsisDetails;

            String synopsisParameters = dis.readUTF();  // reading String
            synopsisInfo[2] = synopsisParameters;

            // Now read the CountMinSketch data
            byte[] uncompressedData = readAllBytes(dis);  // Read all bytes from the DataInputStream
            CountMinSketch cm = CountMinSketch.deserialize(uncompressedData);

            // Construct the CountMin object
            CountMin countMin = new CountMin(synopsisInfo);
            countMin.cm = cm; // Assign the CountMinSketch object to the CountMin instance

            return countMin;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] readAllBytes(DataInputStream dis) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] byteBuffer = new byte[8192];
        int bytesRead;
        while ((bytesRead = dis.read(byteBuffer)) != -1) {
            buffer.write(byteBuffer, 0, bytesRead);
        }
        return buffer.toByteArray();
    }
    @Override
    public void close() {
        // Implement if needed
    }
}
