package org.kafkaApp.Serdes.SynopsesSerdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.kafkaApp.Serdes.SynopsesSerdes.AMSSketch.AMSSketchSynopsisSerde;
import org.kafkaApp.Serdes.SynopsesSerdes.BloomFilter.BloomFilterSerde;
import org.kafkaApp.Serdes.SynopsesSerdes.CountMin.CountMinSerde;
import org.kafkaApp.Serdes.SynopsesSerdes.DFTSynopsis.DFTSynopsisSerde;
import org.kafkaApp.Serdes.SynopsesSerdes.GKQuantiles.GKQuantilesSynopsisSerdes;
import org.kafkaApp.Serdes.SynopsesSerdes.HyperLogLog.HyperLogLogSerde;
import org.kafkaApp.Serdes.SynopsesSerdes.LSH.LSHSynopsisSerde;
import org.kafkaApp.Serdes.SynopsesSerdes.LossyCounting.LossyCountingSynopsisSerdes;
import org.kafkaApp.Serdes.SynopsesSerdes.StickySampling.StickySamplingSynopsisSerdes;
import org.kafkaApp.Serdes.SynopsesSerdes.WindowSketchQuantiles.WindowSketchQuantilesSynopsisSerdes;
import org.kafkaApp.Synopses.AMSSketch.AMSSketchSynopsis;
import org.kafkaApp.Synopses.BloomFilter.BloomFilterSynopsis;
import org.kafkaApp.Synopses.CountMin;
import org.kafkaApp.Synopses.DFT.DFTSynopsis;
import org.kafkaApp.Synopses.GKQuantiles.GKQuantilesSynopsis;
import org.kafkaApp.Synopses.HyperLogLog.HyperLogLogSynopsis;
import org.kafkaApp.Synopses.LSH.LSHsynopsis;
import org.kafkaApp.Synopses.LossyCounting.LossyCountingSynopsis;
import org.kafkaApp.Synopses.StickySampling.StickySamplingSynopsis;
import org.kafkaApp.Synopses.Synopsis;
import org.kafkaApp.Synopses.WindowSketchQuantiles.WindowSketchQuantilesSynopsis;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

public class SynopsesSerdes implements Serde<Synopsis> {
    private final CountMinSerde countMinSerde = new CountMinSerde();
    private final HyperLogLogSerde hyperLogLogSerde = new HyperLogLogSerde();
    private final BloomFilterSerde bloomFilterSerde = new BloomFilterSerde();
    private final DFTSynopsisSerde dftSynopsisSerde = new DFTSynopsisSerde();
    private final LossyCountingSynopsisSerdes lossyCountingSynopsisSerdes = new LossyCountingSynopsisSerdes();
    private final StickySamplingSynopsisSerdes stickySamplingSynopsisSerdes = new StickySamplingSynopsisSerdes();
    private final AMSSketchSynopsisSerde amsSketchSynopsisSerde = new AMSSketchSynopsisSerde();
    private final GKQuantilesSynopsisSerdes gkQuantilesSynopsisSerdes = new GKQuantilesSynopsisSerdes();
    private final LSHSynopsisSerde LSHSynopsisSerdes = new LSHSynopsisSerde();
    private final WindowSketchQuantilesSynopsisSerdes windowSketchQuantilesSynopsisSerdes = new WindowSketchQuantilesSynopsisSerdes();

    @Override
    public Serializer<Synopsis> serializer() {
        return (topic, data) -> {
            if (data instanceof CountMin) {
                return countMinSerde.serializer().serialize(topic, (CountMin) data);
            } else if (data instanceof HyperLogLogSynopsis) {
                return hyperLogLogSerde.serializer().serialize(topic, (HyperLogLogSynopsis) data);
            } else if (data instanceof BloomFilterSynopsis) {
                return bloomFilterSerde.serializer().serialize(topic, (BloomFilterSynopsis) data);
            } else if (data instanceof DFTSynopsis) {
                return dftSynopsisSerde.serializer().serialize(topic, (DFTSynopsis) data);
            } else if (data instanceof LossyCountingSynopsis) {
                return lossyCountingSynopsisSerdes.serializer().serialize(topic, (LossyCountingSynopsis) data);
            }else if (data instanceof StickySamplingSynopsis) {
                return stickySamplingSynopsisSerdes.serializer().serialize(topic, (StickySamplingSynopsis) data);
            }
            else if (data instanceof AMSSketchSynopsis) {
                return amsSketchSynopsisSerde.serializer().serialize(topic, (AMSSketchSynopsis) data);
            }
            else if (data instanceof GKQuantilesSynopsis) {
                return gkQuantilesSynopsisSerdes.serializer().serialize(topic, (GKQuantilesSynopsis) data);
            }
            else if (data instanceof LSHsynopsis) {
                return LSHSynopsisSerdes.serializer().serialize(topic, (LSHsynopsis) data);
            }
            else if (data instanceof WindowSketchQuantilesSynopsis) {
                return windowSketchQuantilesSynopsisSerdes.serializer().serialize(topic, (WindowSketchQuantilesSynopsis) data);
            }
            else {
                throw new IllegalArgumentException("Unknown Synopsis subclass");
            }
        };
    }

    @Override
    public Deserializer<Synopsis> deserializer() {
        return (topic, data) -> {
            int synopsesID;
            try (ByteArrayInputStream byteStream = new ByteArrayInputStream(data);
                 GZIPInputStream gzipStream = new GZIPInputStream(byteStream);
                 DataInputStream dis = new DataInputStream(gzipStream)) {
                synopsesID = dis.readInt();
            } catch (IOException e) {
                throw new RuntimeException("Error reading synopsesID from serialized data", e);
            }

            switch (synopsesID) {
                case 1:
                    return countMinSerde.deserializer().deserialize(topic, data);
                case 2:
                    return hyperLogLogSerde.deserializer().deserialize(topic, data);
                case 3:
                    return bloomFilterSerde.deserializer().deserialize(topic, data);
                case 4:
                    return dftSynopsisSerde.deserializer().deserialize(topic, data);
                case 5:
                    return lossyCountingSynopsisSerdes.deserializer().deserialize(topic, data);
                case 6:
                    return stickySamplingSynopsisSerdes.deserializer().deserialize(topic, data);
                case 7:
                    return amsSketchSynopsisSerde.deserializer().deserialize(topic, data);
                case 8:
                    return gkQuantilesSynopsisSerdes.deserializer().deserialize(topic, data);
                case 9:
                    return LSHSynopsisSerdes.deserializer().deserialize(topic, data);
                case 10:
                    return windowSketchQuantilesSynopsisSerdes.deserializer().deserialize(topic, data);
                default:
                    throw new IllegalArgumentException("Unknown synopsesID: " + synopsesID);
            }
        };
    }
}
