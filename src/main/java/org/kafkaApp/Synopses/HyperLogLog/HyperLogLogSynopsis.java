package org.kafkaApp.Synopses.HyperLogLog;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import org.apache.kafka.common.serialization.Serde;
import org.kafkaApp.Serdes.SynopsesSerdes.HyperLogLog.HyperLogLogSerde;
import org.kafkaApp.Synopses.Synopsis;


public class HyperLogLogSynopsis extends Synopsis {

    public HyperLogLog hll;
    public HyperLogLogSynopsis() {
    }
    public HyperLogLogSynopsis(String[] synopsisElements) {
        super(Integer.parseInt(synopsisElements[0]), synopsisElements[1], synopsisElements[2]);
        String[] splitParams = synopsisElements[2].split(",");
        double rsd = Double.parseDouble(splitParams[0]);
        this.hll = new HyperLogLog(rsd);
    }

    @Override
    public Serde<HyperLogLogSynopsis> serde() {
        return new HyperLogLogSerde();
    }

    @Override
    public void add(Object obj) {
        // TODO Auto-generated method stub
        this.hll.offer(obj);
    }

    @Override
    public Object estimate(Object obj) {
        // TODO Auto-generated method stub
        return this.hll.cardinality();
    }

    @Override
    public Synopsis merge(Synopsis synopsis) {
        if (!(synopsis instanceof HyperLogLogSynopsis)) {
            throw new IllegalArgumentException("It's not the required synopsis");
        }
        HyperLogLogSynopsis otherHyperLogLog = (HyperLogLogSynopsis) synopsis;
        HyperLogLog mergedHLL;
        try {
            mergedHLL = (HyperLogLog) this.hll.merge(otherHyperLogLog.hll);
            //System.out.println("size of merged synopsis"+mergedHLL.sizeof());
        } catch (CardinalityMergeException e) {
            throw new RuntimeException("Error when merging HyperLogLog instances.", e);
        }
        HyperLogLogSynopsis mergedHyperLogLogSynopsis = new HyperLogLogSynopsis();
        mergedHyperLogLogSynopsis.hll = mergedHLL;
        return mergedHyperLogLogSynopsis;
    }

    @Override
    public long size() {
        return hll.sizeof();
    } //memory sized is used to store the register set.

}
