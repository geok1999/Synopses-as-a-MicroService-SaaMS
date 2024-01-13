package org.kafkaApp.Synopses.DFT;



import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class windowDFTSerializer {

    public byte[] serialize(windowDFT windowdft) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(windowdft);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Error serializing windowDFT object", e);
        }
    }
}