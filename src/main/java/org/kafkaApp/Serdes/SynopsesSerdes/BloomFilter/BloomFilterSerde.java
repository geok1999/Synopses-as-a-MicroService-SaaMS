package org.kafkaApp.Serdes.SynopsesSerdes.BloomFilter;

import com.clearspring.analytics.stream.membership.BloomFilter;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.kafkaApp.Synopses.BloomFilter.BloomFilterSynopsis;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class BloomFilterSerde implements Serde<BloomFilterSynopsis> {

    @Override
    public Serializer<BloomFilterSynopsis> serializer() {
        return (topic, data) -> {
            try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
                 GZIPOutputStream gzipStream = new GZIPOutputStream(byteStream);
                 DataOutputStream dos = new DataOutputStream(gzipStream)) {

                dos.writeInt(data.getSynopsesID());  // writing int
                dos.writeUTF(data.getSynopsisDetails());  // writing String
                dos.writeUTF(data.getSynopsisParameters());  // writing String

                dos.write(BloomFilter.serialize(data.bloomFilter));

                dos.flush(); // Ensure all data is written out to gzipStream.

                gzipStream.finish(); // Ensure all compressed data is written out to byteStream.
                return byteStream.toByteArray();

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public Deserializer<BloomFilterSynopsis> deserializer() {
        return (topic, data) -> {
            try (ByteArrayInputStream byteStream = new ByteArrayInputStream(data);
                 GZIPInputStream gzipStream = new GZIPInputStream(byteStream);
                 DataInputStream dis = new DataInputStream(gzipStream)) {

                String[] synopsisInfo = new String[3];
                // First read the synopsis fields
                int synopsesID = dis.readInt();  // reading int
                synopsisInfo[0] = Integer.toString(synopsesID);

                String synopsisDetails = dis.readUTF();  // reading String
                synopsisInfo[1] = synopsisDetails;

                String synopsisParameters = dis.readUTF();  // reading String
                synopsisInfo[2] = synopsisParameters;

                // Now read the BloomFilter data
                byte[] uncompressedData = readAllBytes(dis);  // Read all bytes from the DataInputStream
                BloomFilter bloomFilter = BloomFilter.deserialize(uncompressedData);

                // Construct the BloomFilterSynopsis object
                BloomFilterSynopsis bloomFilterSynopsis = new BloomFilterSynopsis(synopsisInfo);
                bloomFilterSynopsis.bloomFilter=bloomFilter; // Assign the BloomFilter object to the BloomFilterSynopsis instance

                return bloomFilterSynopsis;

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private byte[] readAllBytes(DataInputStream dis) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int bytesRead;
        byte[] dataBuffer = new byte[1024];
        while ((bytesRead = dis.read(dataBuffer, 0, dataBuffer.length)) != -1) {
            buffer.write(dataBuffer, 0, bytesRead);
        }
        return buffer.toByteArray();
    }

}
