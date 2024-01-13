package org.kafkaApp.Synopses.LossyCounting;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class LossyCountingSerializer<T> {

    public byte[] serialize(LossyCounting<T> lossyCounting) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(lossyCounting);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Error serializing LossyCounting object", e);
        }
    }
}