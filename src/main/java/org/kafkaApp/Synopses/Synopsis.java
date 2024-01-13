package org.kafkaApp.Synopses;

import org.apache.kafka.common.serialization.Serde;

public abstract class Synopsis {

    private int SynopsesID;
    private String synopsisDetails;
    private String synopsisParameters;

public Synopsis() {
    }
    public Synopsis(int SynopsesID,String synopsisDetails,String synopsisParameters) {
        this.SynopsesID = SynopsesID;
        this.synopsisDetails=synopsisDetails;
        this.synopsisParameters=synopsisParameters;
    }
    public abstract Serde<? extends Synopsis> serde();
    public abstract void add(Object obj);
    public abstract Object estimate(Object obj);
    public abstract Synopsis merge(Synopsis synopsis); //Synopsis...
    public abstract long size();

    public int getSynopsesID() {
        return SynopsesID;
    }

    public void setSynopsesID(int synopsesID) {
        SynopsesID = synopsesID;
    }

    public String getSynopsisDetails() {
        return synopsisDetails;
    }

    public void setSynopsisDetails(String synopsisDetails) {
        this.synopsisDetails = synopsisDetails;
    }

    public String getSynopsisParameters() {
        return synopsisParameters;
    }

    public void setSynopsisParameters(String synopsisParameters) {
        this.synopsisParameters = synopsisParameters;
    }
}
