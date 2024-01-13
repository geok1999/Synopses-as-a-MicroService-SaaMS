package org.kafkaApp.Structure;

public class StructureWithMetaDataParameters <T> {
    private T req;
    private int metaData1;
    private int metaData2;
    private int metaData3;

    private boolean mergedSynopses;
    public StructureWithMetaDataParameters() {
    }

    public StructureWithMetaDataParameters(T req, int metaData1, int metaData2, int metaData3, boolean mergedSynopses) {
        this.req = req;
        this.metaData1 = metaData1;
        this.metaData2 = metaData2;
        this.metaData3 = metaData3;
        this.mergedSynopses = mergedSynopses;
    }


    public int getMetaData1() {
        return metaData1;
    }

    public void setMetaData1(int metaData1) {
        this.metaData1 = metaData1;
    }

    public int getMetaData2() {
        return metaData2;
    }

    public void setMetaData2(int metaData2) {
        this.metaData2 = metaData2;
    }

    public T getReq() {
        return req;
    }

    public int getMetaData3() {
        return metaData3;
    }

    public void setMetaData3(int metaData3) {
        this.metaData3 = metaData3;
    }


    public boolean isMergedSynopses() {
        return mergedSynopses;
    }

    public void setMergedSynopses(boolean mergedSynopses) {
        this.mergedSynopses = mergedSynopses;
    }
}
