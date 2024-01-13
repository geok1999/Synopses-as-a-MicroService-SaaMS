package org.kafkaApp.Structure;

public class RequestStructure {
    private String streamID;
    private int synopsisID;
    private int requestID;
    private String dataSetKey;
    private Object[] param;
    private int partition;
    private int noOfP;
    private int uid;

    public RequestStructure() {}

    public RequestStructure(String streamID, int synopsisID, int requestID, String dataSetKey, Object[] param,int partition, int noOfP, int uid) {
        this.streamID = streamID;
        this.synopsisID = synopsisID;
        this.requestID = requestID;
        this.dataSetKey = dataSetKey;
        this.param = param;
        this.partition=partition;

        this.noOfP = noOfP;
        this.uid = uid;
    }

    public String getStreamID() {
        return streamID;
    }

    public void setStreamID(String streamID) {
        this.streamID = streamID;
    }

    public int getSynopsisID() {
        return synopsisID;
    }

    public void setSynopsisID(int synopsisID) {
        this.synopsisID = synopsisID;
    }

    public int getRequestID() {
        return requestID;
    }

    public void setRequestID(int requestID) {
        this.requestID = requestID;
    }

    public String getDataSetKey() {
        return dataSetKey;
    }

    public void setDataSetKey(String dataSetKey) {
        this.dataSetKey = dataSetKey;
    }

    public Object[] getParam() {
        return param;
    }

    public void setParam(Object[] param) {
        this.param = param;
    }

    public int getNoOfP() {
        return noOfP;
    }

    public void setNoOfP(int noOfP) {
        this.noOfP = noOfP;
    }

    public int getUid() {
        return uid;
    }

    public void setUid(int uid) {
        this.uid = uid;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }
}
