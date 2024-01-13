package org.kafkaApp.Serdes.SynopsesSerdes.HyperLogLog;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.kafkaApp.Synopses.HyperLogLog.HyperLogLogSynopsis;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class HyperLogLogSerde implements Serde<HyperLogLogSynopsis> {

    @Override
    public Serializer<HyperLogLogSynopsis> serializer() {
        return (topic, data) -> {
            try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
                 GZIPOutputStream gzipStream = new GZIPOutputStream(byteStream);
                 DataOutputStream dataStream = new DataOutputStream(gzipStream)) {

                dataStream.writeInt(data.getSynopsesID());  // writing int
                dataStream.writeUTF(data.getSynopsisDetails());  // writing String
                dataStream.writeUTF(data.getSynopsisParameters());  // writing String

                // Serialize HyperLogLog
                byte[] hllBytes = serializeHyperLogLog(data.hll);
                dataStream.writeInt(hllBytes.length); // writing length of serialized hll
                dataStream.write(hllBytes); // writing serialized hll

                dataStream.flush();
                gzipStream.finish();
                return byteStream.toByteArray();

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }


    @Override
    public Deserializer<HyperLogLogSynopsis> deserializer() {
        return (topic, data) -> {
            try (ByteArrayInputStream byteStream = new ByteArrayInputStream(data);
                 GZIPInputStream gzipStream = new GZIPInputStream(byteStream);
                 DataInputStream dataStream = new DataInputStream(gzipStream)) {

                String[] synopsisInfo = new String[3];
                int synopsesID = dataStream.readInt();  // reading int
                synopsisInfo[0] = Integer.toString(synopsesID);

                String synopsisDetails = dataStream.readUTF();  // reading String
                synopsisInfo[1] = synopsisDetails;

                String synopsisParameters = dataStream.readUTF();  // reading String
                synopsisInfo[2] = synopsisParameters;

                // Deserialize HyperLogLog
                int hllBytesLength = dataStream.readInt(); // reading length of serialized hll
                byte[] hllBytes = new byte[hllBytesLength];
                dataStream.readFully(hllBytes); // reading serialized hll
                HyperLogLog hll = deserializeHyperLogLog(hllBytes);

                // Construct the HyperLogLogSynopsis object
                HyperLogLogSynopsis hyperLogLogSynopsis = new HyperLogLogSynopsis(synopsisInfo);
                hyperLogLogSynopsis.hll = hll;

                return hyperLogLogSynopsis;

            } catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private byte[] serializeHyperLogLog(HyperLogLog hll) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        ObjectOutputStream objectStream = new ObjectOutputStream(byteStream);
        objectStream.writeObject(hll);
        objectStream.close();
        return byteStream.toByteArray();
    }

    private HyperLogLog deserializeHyperLogLog(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
        ObjectInputStream objectStream = new ObjectInputStream(byteStream);
        HyperLogLog hll = (HyperLogLog) objectStream.readObject();
        objectStream.close();
        return hll;
    }
}
