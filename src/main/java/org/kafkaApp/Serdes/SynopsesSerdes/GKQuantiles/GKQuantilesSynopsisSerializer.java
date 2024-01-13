package org.kafkaApp.Serdes.SynopsesSerdes.GKQuantiles;

import org.apache.kafka.common.serialization.Serializer;
import org.kafkaApp.Synopses.GKQuantiles.GKQuantilesSerializer;
import org.kafkaApp.Synopses.GKQuantiles.GKQuantilesSynopsis;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

public class GKQuantilesSynopsisSerializer implements Serializer<GKQuantilesSynopsis> {
    private final GKQuantilesSerializer gkQuantilesSerializer = new GKQuantilesSerializer();
    @Override
    public byte[] serialize(String topic, GKQuantilesSynopsis data) {
        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
             GZIPOutputStream gzipStream = new GZIPOutputStream(byteStream);
             DataOutputStream dataStream = new DataOutputStream(gzipStream)) {

            dataStream.writeInt(data.getSynopsesID());  // writing int
            dataStream.writeUTF(data.getSynopsisDetails());  // writing String
            dataStream.writeUTF(data.getSynopsisParameters());  // writing String
            dataStream.write(gkQuantilesSerializer.serialize(data.gkq));

            dataStream.flush();
            gzipStream.finish();
            return byteStream.toByteArray();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
