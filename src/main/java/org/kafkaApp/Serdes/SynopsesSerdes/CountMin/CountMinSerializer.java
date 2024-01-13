package org.kafkaApp.Serdes.SynopsesSerdes.CountMin;

import com.clearspring.analytics.stream.frequency.CountMinSketch;
import org.apache.kafka.common.serialization.Serializer;
import org.kafkaApp.Synopses.CountMin;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

public class CountMinSerializer implements Serializer<CountMin> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Implement if needed
    }

    @Override
    public byte[] serialize(String topic, CountMin data) {
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
    }

    @Override
    public void close() {
        // Implement if needed
    }
}
