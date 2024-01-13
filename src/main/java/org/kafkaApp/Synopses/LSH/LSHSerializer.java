package org.kafkaApp.Synopses.LSH;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class LSHSerializer {

    public byte[] serialize(LSH lsh) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(lsh);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Error serializing LSH object", e);
        }
    }
}
