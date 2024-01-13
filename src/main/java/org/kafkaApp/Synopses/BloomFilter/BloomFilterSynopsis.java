package org.kafkaApp.Synopses.BloomFilter;

import com.clearspring.analytics.stream.membership.BloomFilter;
import org.apache.kafka.common.serialization.Serde;
import org.kafkaApp.Serdes.SynopsesSerdes.BloomFilter.BloomFilterSerde;
import org.kafkaApp.Synopses.Synopsis;

public class BloomFilterSynopsis extends Synopsis {

    public BloomFilter bloomFilter;

    public BloomFilterSynopsis() {
    }

    public BloomFilterSynopsis(String[] synopsisElements) {
        super(Integer.parseInt(synopsisElements[0]), synopsisElements[1], synopsisElements[2]);
        String[] splitParams = synopsisElements[2].split(",");
        int numElements = Integer.parseInt(splitParams[0]);
        double fpp = Double.parseDouble(splitParams[1]);
        this.bloomFilter = new BloomFilter(numElements, fpp);
    }

    @Override
    public Serde<BloomFilterSynopsis> serde() {
        return  new BloomFilterSerde();
    }

    @Override
    public void add(Object obj) {
        this.bloomFilter.add(obj.toString().getBytes());
    }

    @Override
    public Object estimate(Object obj) {
        return this.bloomFilter.isPresent(obj.toString().getBytes());
    }

    @Override
    public Synopsis merge(Synopsis synopsis) {
        if (!(synopsis instanceof BloomFilterSynopsis)) {
            throw new IllegalArgumentException("It's not the required synopsis");
        }

        BloomFilterSynopsis otherBloomFilterSynopsis = (BloomFilterSynopsis) synopsis;

        // Ensure both bloom filters are of the same size
        if (this.bloomFilter.getHashCount() != otherBloomFilterSynopsis.bloomFilter.getHashCount()) {
            throw new IllegalArgumentException("Cannot merge bloom filters of different sizes.");
        }

        // Merge bloom filters
        BloomFilter mergedBloomFilter = (BloomFilter) this.bloomFilter.merge(otherBloomFilterSynopsis.bloomFilter);

        // Create a new BloomFilterSynopsis with the merged BloomFilter and other fields copied from the current BloomFilterSynopsis
        BloomFilterSynopsis mergedSynopsis = new BloomFilterSynopsis();
        mergedSynopsis.bloomFilter = mergedBloomFilter;

        return mergedSynopsis;
    }

    @Override
    public long size() {
        return 0;
    }

}