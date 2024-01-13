package org.kafkaApp.Serdes.SynopsesSerdes.DFTSynopsis;


import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.kafkaApp.Synopses.DFT.DFTSynopsis;
import org.kafkaApp.Synopses.DFT.windowDFT;
import org.kafkaApp.Synopses.DFT.windowDFTDeserializer;
import org.kafkaApp.Synopses.DFT.windowDFTSerializer;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class DFTSynopsisSerde implements Serde<DFTSynopsis> {
    private final windowDFTSerializer windowdftSerializer = new windowDFTSerializer();
    private final windowDFTDeserializer windowdftDeserializer = new windowDFTDeserializer();
    @Override
    public Serializer<DFTSynopsis> serializer() {
        return (topic, data) -> {
            try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
                 GZIPOutputStream gzipStream = new GZIPOutputStream(byteStream);
                 DataOutputStream dataStream = new DataOutputStream(gzipStream)) {

                dataStream.writeInt(data.getSynopsesID());  // writing int
                dataStream.writeUTF(data.getSynopsisDetails());  // writing String
                dataStream.writeUTF(data.getSynopsisParameters());  // writing String

                // Serialize windowDFT using the method from the windowDFT class
                dataStream.write(windowdftSerializer.serialize(data.ts));

                dataStream.flush();
                gzipStream.finish();
                return byteStream.toByteArray();

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }


@Override
    public Deserializer<DFTSynopsis> deserializer() {
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
            windowDFT windowdft = windowdftDeserializer.deserialize(uncompressedData);

            // Construct the LossyCountingSynopsis object
            DFTSynopsis dftynopsis = new DFTSynopsis(synopsisInfo);
            dftynopsis.ts = windowdft;

            return dftynopsis;

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
