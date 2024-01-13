package org.kafkaApp.Serdes.SynopsesSerdes.SynopsisAndParameters;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.kafkaApp.Serdes.SynopsesSerdes.SynopsesSerdes;
import org.kafkaApp.Structure.SynopsisAndParameters;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class SynopsisAndParametersDeserializer implements Deserializer<SynopsisAndParameters> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final SynopsesSerdes synopsesSerdes = new SynopsesSerdes();

    @Override
    public SynopsisAndParameters deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;  // null check in case of tombstone messages
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);

        int synopsisBytesLength = buffer.getInt();
        byte[] synopsisBytes = null;
        if (synopsisBytesLength > 0) {
            synopsisBytes = new byte[synopsisBytesLength];
            buffer.get(synopsisBytes);
        }

        byte[] parametersBytes = new byte[buffer.remaining() - 4];
        buffer.get(parametersBytes);

        int countReqProc = buffer.getInt();

        Object[] parameters;
        try {
            parameters = objectMapper.readValue(parametersBytes, Object[].class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize parameters", e);
        }

        SynopsisAndParameters synopsisAndParameters = new SynopsisAndParameters(null, parameters, countReqProc);
        if (synopsisBytes != null) {
            synopsisAndParameters.setSynopsis(synopsesSerdes.deserializer().deserialize(topic, synopsisBytes));
        }
        return synopsisAndParameters;
    }

    @Override
    public void close() {
        // Implement if needed
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Implement if needed
    }
}