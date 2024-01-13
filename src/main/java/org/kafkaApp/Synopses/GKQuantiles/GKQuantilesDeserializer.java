package org.kafkaApp.Synopses.GKQuantiles;



import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class GKQuantilesDeserializer {

    public GKQuantiles deserialize(byte[] data) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
             ObjectInputStream in = new ObjectInputStream(bis)) {
            return (GKQuantiles) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Error deserializing GKQuantiles object", e);
        }
    }
}