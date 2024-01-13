package org.kafkaApp.Serdes.SynopsesSerdes.LSH;

import org.apache.kafka.common.serialization.Serializer;
import org.kafkaApp.Synopses.LSH.LSHSerializer;
import org.kafkaApp.Synopses.LSH.LSHsynopsis;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

public class LSHSynopsisSerializer implements Serializer<LSHsynopsis> {
    private final LSHSerializer lshSerializer = new LSHSerializer();
    @Override
    public byte[] serialize(String topic, LSHsynopsis data) {
        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
             GZIPOutputStream gzipStream = new GZIPOutputStream(byteStream);
             DataOutputStream dataStream = new DataOutputStream(gzipStream)) {

            dataStream.writeInt(data.getSynopsesID());  // writing int
            dataStream.writeUTF(data.getSynopsisDetails());  // writing String
            dataStream.writeUTF(data.getSynopsisParameters());  // writing String
            dataStream.write(lshSerializer.serialize(data.lsh));

            dataStream.flush();
            gzipStream.finish();
            return byteStream.toByteArray();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
