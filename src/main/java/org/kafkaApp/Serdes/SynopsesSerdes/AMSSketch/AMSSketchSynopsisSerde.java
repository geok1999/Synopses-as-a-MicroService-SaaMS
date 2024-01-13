package org.kafkaApp.Serdes.SynopsesSerdes.AMSSketch;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.kafkaApp.Synopses.AMSSketch.AMSSketch;
import org.kafkaApp.Synopses.AMSSketch.AMSSketchDeserializer;
import org.kafkaApp.Synopses.AMSSketch.AMSSketchSerializer;
import org.kafkaApp.Synopses.AMSSketch.AMSSketchSynopsis;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class AMSSketchSynopsisSerde implements Serde<AMSSketchSynopsis> {

    private final AMSSketchSerializer amsSketchSerializer = new AMSSketchSerializer();
    private final AMSSketchDeserializer amsSketchDeserializer = new AMSSketchDeserializer();

    @Override
    public Serializer<AMSSketchSynopsis> serializer() {
        return (topic, data) -> {
            try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
                 GZIPOutputStream gzipStream = new GZIPOutputStream(byteStream);
                 DataOutputStream dataStream = new DataOutputStream(gzipStream)) {

                dataStream.writeInt(data.getSynopsesID());  // writing int
                dataStream.writeUTF(data.getSynopsisDetails());  // writing String
                dataStream.writeUTF(data.getSynopsisParameters());  // writing String
                dataStream.write(amsSketchSerializer.serialize(data.amss));

                dataStream.flush();
                gzipStream.finish();
                return byteStream.toByteArray();

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public Deserializer<AMSSketchSynopsis> deserializer() {
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
                AMSSketch amsSketch = amsSketchDeserializer.deserialize(uncompressedData);

                // Construct the LossyCountingSynopsis object
                AMSSketchSynopsis amsSketchSynopsis = new AMSSketchSynopsis(synopsisInfo);
                amsSketchSynopsis.amss = amsSketch;

                return amsSketchSynopsis;


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
