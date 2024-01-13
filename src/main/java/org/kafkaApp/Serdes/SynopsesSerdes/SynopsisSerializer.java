package org.kafkaApp.Serdes.SynopsesSerdes;

import org.apache.kafka.common.serialization.Serializer;
import org.kafkaApp.Serdes.SynopsesSerdes.AMSSketch.AMSSketchSynopsisSerializer;
import org.kafkaApp.Serdes.SynopsesSerdes.BloomFilter.BloomFilterSerializer;
import org.kafkaApp.Serdes.SynopsesSerdes.CountMin.CountMinSerializer;
import org.kafkaApp.Serdes.SynopsesSerdes.DFTSynopsis.DFTSynopsisSerializer;
import org.kafkaApp.Serdes.SynopsesSerdes.GKQuantiles.GKQuantilesSynopsisSerializer;
import org.kafkaApp.Serdes.SynopsesSerdes.HyperLogLog.HyperLogLogSynopsisSerializer;
import org.kafkaApp.Serdes.SynopsesSerdes.LSH.LSHSynopsisSerializer;
import org.kafkaApp.Serdes.SynopsesSerdes.LossyCounting.LossyCountingSynopsisSerializer;
import org.kafkaApp.Serdes.SynopsesSerdes.StickySampling.StickySamplingSynopsisSerializer;
import org.kafkaApp.Serdes.SynopsesSerdes.WindowSketchQuantiles.WindowSketchQuantilesSynopsisSerializer;
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

public class SynopsisSerializer implements Serializer<Synopsis> {
    private final CountMinSerializer countMinSerializer = new CountMinSerializer();
    private final HyperLogLogSynopsisSerializer hyperLogLogSynopsisSerializer = new HyperLogLogSynopsisSerializer();
    private final BloomFilterSerializer bloomFilterSerializerSerializer = new BloomFilterSerializer();
    private final DFTSynopsisSerializer dftSynopsisSerializerSerializer = new DFTSynopsisSerializer();
    private final LossyCountingSynopsisSerializer lossyCountingSynopsisSerializer = new LossyCountingSynopsisSerializer();
    private final StickySamplingSynopsisSerializer stickySamplingSynopsisSerializer = new StickySamplingSynopsisSerializer();
    private final AMSSketchSynopsisSerializer amsSketchSynopsisSerializer = new AMSSketchSynopsisSerializer();
    private final GKQuantilesSynopsisSerializer gkQuantilesSynopsisSerializer = new GKQuantilesSynopsisSerializer();

    private final LSHSynopsisSerializer LSHSynopsisSerializer = new LSHSynopsisSerializer();
    private final WindowSketchQuantilesSynopsisSerializer windowSketchQuantilesSynopsisSerializer = new WindowSketchQuantilesSynopsisSerializer();

    @Override
    public byte[] serialize(String topic, Synopsis data) {
        if (data instanceof CountMin) {
            return countMinSerializer.serialize(topic, (CountMin) data);
        }
        else if (data instanceof HyperLogLogSynopsis) {
            return hyperLogLogSynopsisSerializer.serialize(topic, (HyperLogLogSynopsis) data);
        }
        else if (data instanceof BloomFilterSynopsis) {
            return bloomFilterSerializerSerializer.serialize(topic, (BloomFilterSynopsis) data);
        }
        else if (data instanceof DFTSynopsis) {
            return dftSynopsisSerializerSerializer.serialize(topic, (DFTSynopsis) data);
        } else if (data instanceof LossyCountingSynopsis) {
            return lossyCountingSynopsisSerializer.serialize(topic, (LossyCountingSynopsis) data);
        }else if (data instanceof StickySamplingSynopsis) {
            return stickySamplingSynopsisSerializer.serialize(topic, (StickySamplingSynopsis) data);
        }
        else if (data instanceof AMSSketchSynopsis) {
            return amsSketchSynopsisSerializer.serialize(topic, (AMSSketchSynopsis) data);
        }
        else if (data instanceof GKQuantilesSynopsis) {
            return gkQuantilesSynopsisSerializer.serialize(topic, (GKQuantilesSynopsis) data);
        }
        else if (data instanceof LSHsynopsis) {
            return LSHSynopsisSerializer.serialize(topic, (LSHsynopsis) data);
        }
        else if (data instanceof WindowSketchQuantilesSynopsis) {
            return windowSketchQuantilesSynopsisSerializer.serialize(topic, (WindowSketchQuantilesSynopsis) data);
        }
        else {
            throw new IllegalArgumentException("Unknown Synopsis subclass");
        }
    }

}
