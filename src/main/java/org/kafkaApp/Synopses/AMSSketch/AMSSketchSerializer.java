package org.kafkaApp.Synopses.AMSSketch;



import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class AMSSketchSerializer {

    public byte[] serialize(AMSSketch stickySampling) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(stickySampling);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Error serializing AMSSketch object", e);
        }
    }
}