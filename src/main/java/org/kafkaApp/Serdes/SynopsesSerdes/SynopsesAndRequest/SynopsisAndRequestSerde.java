package org.kafkaApp.Serdes.SynopsesSerdes.SynopsesAndRequest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.kafkaApp.Serdes.Init.RequestStructure.RequestStructureSerde;
import org.kafkaApp.Serdes.SynopsesSerdes.SynopsesSerdes;
import org.kafkaApp.Structure.RequestStructure;
import org.kafkaApp.Structure.SynopsisAndRequest;

import java.nio.ByteBuffer;

public class SynopsisAndRequestSerde implements Serde<SynopsisAndRequest> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final SynopsesSerdes synopsesSerdes = new SynopsesSerdes();
    private final RequestStructureSerde requestStructureSerde = new RequestStructureSerde();
    @Override
    public Serializer<SynopsisAndRequest> serializer() {
        return (topic, data) -> {
            byte[] synopsisBytes = null;
            if (data.getSynopsis() != null) {
                synopsisBytes = synopsesSerdes.serializer().serialize(topic, data.getSynopsis());
            }
            byte[] requestBytes = requestStructureSerde.serializer().serialize(topic, data.getRequest());

            ByteBuffer buffer = ByteBuffer.allocate(4 + (synopsisBytes != null ? synopsisBytes.length : 0) + requestBytes.length);
            if (synopsisBytes != null) {
                buffer.putInt(synopsisBytes.length);
                buffer.put(synopsisBytes);
            } else {
                buffer.putInt(0);
            }
            buffer.put(requestBytes);

            return buffer.array();
        };
    }

    @Override
    public Deserializer<SynopsisAndRequest> deserializer() {
        return (topic, data) -> {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            int synopsisBytesLength = buffer.getInt();
            byte[] synopsisBytes = null;
            if (synopsisBytesLength > 0) {
                synopsisBytes = new byte[synopsisBytesLength];
                buffer.get(synopsisBytes);
            }

            byte[] requestBytes = new byte[buffer.remaining()];
            buffer.get(requestBytes);

            RequestStructure request = requestStructureSerde.deserializer().deserialize(topic, requestBytes);

            SynopsisAndRequest synopsisAndRequest = new SynopsisAndRequest(null, request);
            if (synopsisBytes != null) {
                synopsisAndRequest.setSynopsis(synopsesSerdes.deserializer().deserialize(topic, synopsisBytes));
            }
            return synopsisAndRequest;
        };
    }
}