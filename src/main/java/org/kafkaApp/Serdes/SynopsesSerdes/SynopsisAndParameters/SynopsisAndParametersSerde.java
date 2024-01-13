package org.kafkaApp.Serdes.SynopsesSerdes.SynopsisAndParameters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.kafkaApp.Serdes.SynopsesSerdes.SynopsesSerdes;
import org.kafkaApp.Structure.SynopsisAndParameters;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SynopsisAndParametersSerde implements Serde<SynopsisAndParameters> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final SynopsesSerdes synopsesSerdes = new SynopsesSerdes();

    @Override
    public Serializer<SynopsisAndParameters> serializer() {
        return (topic, data) -> {
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
        };
    }

    @Override
    public Deserializer<SynopsisAndParameters> deserializer() {
        return (topic, data) -> {
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
                throw new RuntimeException(e);
            }

            SynopsisAndParameters synopsisAndParameters = new SynopsisAndParameters(null, parameters, countReqProc);
            if (synopsisBytes != null) {
                synopsisAndParameters.setSynopsis(synopsesSerdes.deserializer().deserialize(topic, synopsisBytes));
            }
            return synopsisAndParameters;
        };
    }
}
   /* @Override
    public Serializer<CountMinAndParameters> serializer() {
        return new Serializer<CountMinAndParameters>() {
            @Override
            public byte[] serialize(String topic, CountMinAndParameters data) {
                byte[] countMinBytes = null;
                if (data.getCountMin() != null) {
                    countMinBytes = countMinSerde.serializer().serialize(topic, data.getCountMin());
                }
                byte[] parametersBytes;

                try {
                    parametersBytes = objectMapper.writeValueAsBytes(data.getParameters());
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }

                ByteBuffer buffer = ByteBuffer.allocate(4 + (countMinBytes != null ? countMinBytes.length : 0) + parametersBytes.length + 4);
                if (countMinBytes != null) {
                    buffer.putInt(countMinBytes.length);
                    buffer.put(countMinBytes);
                } else {
                    buffer.putInt(0);
                }
                buffer.put(parametersBytes);
                buffer.putInt(data.getCountReqProc());

                return buffer.array();
            }
        };
    }

    @Override
    public Deserializer<CountMinAndParameters> deserializer() {
        return new Deserializer<CountMinAndParameters>() {
            @Override
            public CountMinAndParameters deserialize(String topic, byte[] data) {
                ByteBuffer buffer = ByteBuffer.wrap(data);
                int countMinBytesLength = buffer.getInt();
                byte[] countMinBytes = null;
                if (countMinBytesLength > 0) {
                    countMinBytes = new byte[countMinBytesLength];
                    buffer.get(countMinBytes);
                }

                byte[] parametersBytes = new byte[buffer.remaining() - 4];
                buffer.get(parametersBytes);

                int countReqProc = buffer.getInt();

                Object[] parameters;
                try {
                    parameters = objectMapper.readValue(parametersBytes, Object[].class);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                CountMinAndParameters countMinAndParameters = new CountMinAndParameters(null, parameters, countReqProc);
                if (countMinBytes != null) {
                    countMinAndParameters.setCountMin(countMinSerde.deserializer().deserialize(topic, countMinBytes));
                }
                return countMinAndParameters;
            }
        };
    }*/

/*public class CountMinAndParametersSerde implements Serde<CountMinAndParameters> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final CountMinSerde countMinSerde = new CountMinSerde();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<CountMinAndParameters> serializer() {
        return new Serializer<CountMinAndParameters>() {
            @Override
            public byte[] serialize(String topic, CountMinAndParameters data) {
                byte[] countMinBytes = countMinSerde.serializer().serialize(topic, data.getCountMin());
                byte[] parametersBytes;

                try {
                    parametersBytes = objectMapper.writeValueAsBytes(data.getParameters());
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }

                ByteBuffer buffer = ByteBuffer.allocate(4 + countMinBytes.length + parametersBytes.length + 4);
                buffer.putInt(countMinBytes.length);
                buffer.put(countMinBytes);
                buffer.put(parametersBytes);
                buffer.putInt(data.getCountReqProc());

                return buffer.array();
            }
        };
    }

    @Override
    public Deserializer<CountMinAndParameters> deserializer() {
        return new Deserializer<CountMinAndParameters>() {
            @Override
            public CountMinAndParameters deserialize(String topic, byte[] data) {
                ByteBuffer buffer = ByteBuffer.wrap(data);
                int countMinBytesLength = buffer.getInt();
                byte[] countMinBytes = new byte[countMinBytesLength];
                buffer.get(countMinBytes);

                byte[] parametersBytes = new byte[buffer.remaining() - 4];
                buffer.get(parametersBytes);

                int countReqProc = buffer.getInt();

                Object[] parameters;
                try {
                    parameters = objectMapper.readValue(parametersBytes, Object[].class);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                CountMinAndParameters countMinAndParameters = new CountMinAndParameters(null, parameters, countReqProc);
                countMinAndParameters.setCountMin(countMinSerde.deserializer().deserialize(topic, countMinBytes));
                return countMinAndParameters;
            }
        };
    }
}
*/