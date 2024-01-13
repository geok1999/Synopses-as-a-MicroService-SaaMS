package org.kafkaApp.Serdes.SynopsesSerdes.HyperLogLog;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import org.apache.kafka.common.serialization.Serializer;
import org.kafkaApp.Synopses.HyperLogLog.HyperLogLogSynopsis;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

public class HyperLogLogSynopsisSerializer implements Serializer<HyperLogLogSynopsis> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Implement if needed
    }

    @Override
    public byte[] serialize(String topic, HyperLogLogSynopsis data) {
        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
             GZIPOutputStream gzipStream = new GZIPOutputStream(byteStream);
             DataOutputStream dataStream = new DataOutputStream(gzipStream)) {

            dataStream.writeInt(data.getSynopsesID());  // writing int
            dataStream.writeUTF(data.getSynopsisDetails());  // writing String
            dataStream.writeUTF(data.getSynopsisParameters());  // writing String

            byte[] hllBytes = serializeHyperLogLog(data.hll);
            dataStream.writeInt(hllBytes.length); // writing length of serialized hll
            dataStream.write(hllBytes); // writing serialized hll

            dataStream.flush();
            gzipStream.finish();
            return byteStream.toByteArray();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        // Implement if needed
    }
    private byte[] serializeHyperLogLog(HyperLogLog hll) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        ObjectOutputStream objectStream = new ObjectOutputStream(byteStream);
        objectStream.writeObject(hll);
        objectStream.close();
        return byteStream.toByteArray();
    }
}

