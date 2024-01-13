package org.kafkaApp.Serdes.SynopsesSerdes.StickySampling;

import org.apache.kafka.common.serialization.Serializer;
import org.kafkaApp.Synopses.StickySampling.StickySamplingSerializer;
import org.kafkaApp.Synopses.StickySampling.StickySamplingSynopsis;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

public class StickySamplingSynopsisSerializer implements Serializer<StickySamplingSynopsis> {
    private final StickySamplingSerializer<Object> stickySamplingSerializer = new StickySamplingSerializer<>();
    @Override
    public byte[] serialize(String topic, StickySamplingSynopsis data) {
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
    }
}

