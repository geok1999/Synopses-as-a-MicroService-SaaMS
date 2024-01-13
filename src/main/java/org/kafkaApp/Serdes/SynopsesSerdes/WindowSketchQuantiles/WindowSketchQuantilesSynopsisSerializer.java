package org.kafkaApp.Serdes.SynopsesSerdes.WindowSketchQuantiles;

import org.apache.kafka.common.serialization.Serializer;
import org.kafkaApp.Synopses.WindowSketchQuantiles.WindowSketchQuantilesSerializer;
import org.kafkaApp.Synopses.WindowSketchQuantiles.WindowSketchQuantilesSynopsis;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

public class WindowSketchQuantilesSynopsisSerializer implements Serializer<WindowSketchQuantilesSynopsis> {
    private final WindowSketchQuantilesSerializer windowSketchQuantilesSerializer = new WindowSketchQuantilesSerializer();
    @Override
    public byte[] serialize(String topic, WindowSketchQuantilesSynopsis data) {
        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
             GZIPOutputStream gzipStream = new GZIPOutputStream(byteStream);
             DataOutputStream dataStream = new DataOutputStream(gzipStream)) {

            dataStream.writeInt(data.getSynopsesID());  // writing int
            dataStream.writeUTF(data.getSynopsisDetails());  // writing String
            dataStream.writeUTF(data.getSynopsisParameters());  // writing String
            dataStream.write(windowSketchQuantilesSerializer.serialize(data.wsq));

            dataStream.flush();
            gzipStream.finish();
            return byteStream.toByteArray();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
