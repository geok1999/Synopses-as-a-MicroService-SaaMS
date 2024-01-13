package org.kafkaApp.Serdes.SynopsesSerdes.StickySampling;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.kafkaApp.Synopses.StickySampling.StickySampling;
import org.kafkaApp.Synopses.StickySampling.StickySamplingDeserializer;
import org.kafkaApp.Synopses.StickySampling.StickySamplingSerializer;
import org.kafkaApp.Synopses.StickySampling.StickySamplingSynopsis;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class StickySamplingSynopsisSerdes implements Serde<StickySamplingSynopsis> {

    private final StickySamplingSerializer<Object> stickySamplingSerializer = new StickySamplingSerializer<>();
    private final StickySamplingDeserializer<Object> stickySamplingDeserializer = new StickySamplingDeserializer<>();

    @Override
    public Serializer<StickySamplingSynopsis> serializer() {
        return (topic, data) -> {
            try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
                 GZIPOutputStream gzipStream = new GZIPOutputStream(byteStream);
                 DataOutputStream dataStream = new DataOutputStream(gzipStream)) {

                dataStream.writeInt(data.getSynopsesID());  // writing int
                dataStream.writeUTF(data.getSynopsisDetails());  // writing String
                dataStream.writeUTF(data.getSynopsisParameters());  // writing String
                dataStream.write(stickySamplingSerializer.serialize(data.stickySamplings));

                dataStream.flush();
                gzipStream.finish();
                return byteStream.toByteArray();

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public Deserializer<StickySamplingSynopsis> deserializer() {
        return (topic, data) -> {
            try (ByteArrayInputStream byteStream = new ByteArrayInputStream(data);
                 GZIPInputStream gzipStream = new GZIPInputStream(byteStream);
                 DataInputStream dataStream = new DataInputStream(gzipStream);) {
                String[] synopsisInfo = new String[3];
                // First read the synopsis fields
                int synopsesID = dataStream.readInt();  // reading int
                synopsisInfo[0] = Integer.toString(synopsesID);

                String synopsisDetails = dataStream.readUTF();  // reading String
                synopsisInfo[1] = synopsisDetails;

                String synopsisParameters = dataStream.readUTF();  // reading String
                synopsisInfo[2] = synopsisParameters;

                // Now read the CountMinSketch data
                byte[] uncompressedData = readAllBytes(dataStream);  // Read all bytes from the DataInputStream
                StickySampling<Object> stickySampling = stickySamplingDeserializer.deserialize(uncompressedData);

                // Construct the LossyCountingSynopsis object
                StickySamplingSynopsis stickySamplingSynopsis = new StickySamplingSynopsis(synopsisInfo);
                stickySamplingSynopsis.stickySamplings = stickySampling;

                return stickySamplingSynopsis;


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
