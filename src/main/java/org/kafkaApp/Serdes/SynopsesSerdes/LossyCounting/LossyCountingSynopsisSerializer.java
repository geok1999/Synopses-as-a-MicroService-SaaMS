package org.kafkaApp.Serdes.SynopsesSerdes.LossyCounting;

import org.apache.kafka.common.serialization.Serializer;
import org.kafkaApp.Synopses.LossyCounting.LossyCountingSerializer;
import org.kafkaApp.Synopses.LossyCounting.LossyCountingSynopsis;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

public class LossyCountingSynopsisSerializer implements Serializer<LossyCountingSynopsis> {
    private final LossyCountingSerializer<Object> lossyCountingSerializer = new LossyCountingSerializer<>();
    @Override
    public byte[] serialize(String topic, LossyCountingSynopsis data) {
        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
             GZIPOutputStream gzipStream = new GZIPOutputStream(byteStream);
             DataOutputStream dataStream = new DataOutputStream(gzipStream)) {

            dataStream.writeInt(data.getSynopsesID());  // writing int
            dataStream.writeUTF(data.getSynopsisDetails());  // writing String
            dataStream.writeUTF(data.getSynopsisParameters());  // writing String
            dataStream.write(lossyCountingSerializer.serialize(data.sk));

            dataStream.flush();
            gzipStream.finish();
            return byteStream.toByteArray();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

