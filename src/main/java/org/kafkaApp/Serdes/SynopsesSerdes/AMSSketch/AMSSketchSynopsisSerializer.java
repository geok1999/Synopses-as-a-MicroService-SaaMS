package org.kafkaApp.Serdes.SynopsesSerdes.AMSSketch;


import org.apache.kafka.common.serialization.Serializer;
import org.kafkaApp.Synopses.AMSSketch.AMSSketchSerializer;
import org.kafkaApp.Synopses.AMSSketch.AMSSketchSynopsis;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

public class AMSSketchSynopsisSerializer implements Serializer<AMSSketchSynopsis> {
    private final AMSSketchSerializer amsSketchSerializer = new AMSSketchSerializer();
    @Override
    public byte[] serialize(String topic, AMSSketchSynopsis data) {
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
    }
}
