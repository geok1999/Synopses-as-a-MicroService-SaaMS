package org.kafkaApp.Synopses.WindowSketchQuantiles;



import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class WindowSketchQuantilesSerializer {

    public byte[] serialize(WindowSketchQuantiles windowSketchQuantiles) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(windowSketchQuantiles);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Error serializing windowSketchQuantiles object", e);
        }
    }
}