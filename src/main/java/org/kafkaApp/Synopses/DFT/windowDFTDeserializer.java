package org.kafkaApp.Synopses.DFT;



import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class windowDFTDeserializer {

    public windowDFT deserialize(byte[] data) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
             ObjectInputStream in = new ObjectInputStream(bis)) {
            return (windowDFT) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Error deserializing windowDFT object", e);
        }
    }
}