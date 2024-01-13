package org.kafkaApp.Synopses.WindowSketchQuantiles;



import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class WindowSketchQuantilesDeserializer {

    public WindowSketchQuantiles deserialize(byte[] data) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
             ObjectInputStream in = new ObjectInputStream(bis)) {
            return (WindowSketchQuantiles) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Error deserializing windowSketchQuantiles object", e);
        }
    }
}