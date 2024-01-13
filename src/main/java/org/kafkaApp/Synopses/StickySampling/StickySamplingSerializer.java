package org.kafkaApp.Synopses.StickySampling;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class StickySamplingSerializer<T> {

    public byte[] serialize(StickySampling<T> stickySampling) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(stickySampling);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Error serializing StickySampling object", e);
        }
    }
}