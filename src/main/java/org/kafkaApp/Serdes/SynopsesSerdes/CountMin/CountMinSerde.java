package org.kafkaApp.Serdes.SynopsesSerdes.CountMin;

import com.clearspring.analytics.stream.frequency.CountMinSketch;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.kafkaApp.Synopses.CountMin;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class CountMinSerde implements Serde<CountMin> {

    @Override
    public Serializer<CountMin> serializer() {
        return (topic, data) -> {
            try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
                 GZIPOutputStream gzipStream = new GZIPOutputStream(byteStream);
                 DataOutputStream dos = new DataOutputStream(gzipStream)) {


                dos.writeInt(data.getSynopsesID());  // writing int
                dos.writeUTF(data.getSynopsisDetails());  // writing String
                dos.writeUTF(data.getSynopsisParameters());  // writing String

                dos.write(CountMinSketch.serialize(data.cm));


                dos.flush(); // Ensure all data is written out to gzipStream.

                gzipStream.finish(); // Ensure all compressed data is written out to byteStream.
                return byteStream.toByteArray();

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }


    @Override
    public Deserializer<CountMin> deserializer() {
        return (topic, data) -> {
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
        };
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


}
