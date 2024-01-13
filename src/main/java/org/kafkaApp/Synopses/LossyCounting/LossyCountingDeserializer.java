package org.kafkaApp.Synopses.LossyCounting;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class LossyCountingDeserializer<T> {

    public LossyCounting<T> deserialize(byte[] data) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
             ObjectInputStream in = new ObjectInputStream(bis)) {
            return (LossyCounting<T>) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Error deserializing LossyCounting object", e);
        }
    }
}