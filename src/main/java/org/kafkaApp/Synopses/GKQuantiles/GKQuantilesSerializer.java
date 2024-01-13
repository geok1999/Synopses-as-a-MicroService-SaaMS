package org.kafkaApp.Synopses.GKQuantiles;



import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class GKQuantilesSerializer {

    public byte[] serialize(GKQuantiles gkQuantiles) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(gkQuantiles);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Error serializing GKQuantiles object", e);
        }
    }
}