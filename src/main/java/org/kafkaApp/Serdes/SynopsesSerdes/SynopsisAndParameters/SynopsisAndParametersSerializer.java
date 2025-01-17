package org.kafkaApp.Serdes.SynopsesSerdes.SynopsisAndParameters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.kafkaApp.Serdes.SynopsesSerdes.SynopsesSerdes;
import org.kafkaApp.Structure.dto.SynopsisAndParameters;

import java.nio.ByteBuffer;

public class SynopsisAndParametersSerializer implements Serializer<SynopsisAndParameters> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final SynopsesSerdes synopsesSerdes = new SynopsesSerdes();

    @Override
    public byte[] serialize(String topic, SynopsisAndParameters data) {
        byte[] synopsisBytes = null;
        if (data.getSynopsis() != null) {
            synopsisBytes = synopsesSerdes.serializer().serialize(topic, data.getSynopsis());
        }
        byte[] parametersBytes;

        try {
            parametersBytes = objectMapper.writeValueAsBytes(data.getParameters());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        ByteBuffer buffer = ByteBuffer.allocate(4 + (synopsisBytes != null ? synopsisBytes.length : 0) + parametersBytes.length + 4);
        if (synopsisBytes != null) {
            buffer.putInt(synopsisBytes.length);
            buffer.put(synopsisBytes);
        } else {
            buffer.putInt(0);
        }
        buffer.put(parametersBytes);
        buffer.putInt(data.getCountReqProc());

        return buffer.array();
    }
}
