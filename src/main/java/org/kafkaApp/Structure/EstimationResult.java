package org.kafkaApp.Structure;

public class EstimationResult {
    private String estimationVal;

    private int synopsisID;
    private Object[] estimationParams;
    private String StreamID;
    private String DatasetKey;
    private String field;

    public EstimationResult() {
    }

    public EstimationResult(String estimation,int synopsisID, Object[] estimationParams,String StreamID,String DatasetKey,String field) {
        this.estimationVal = estimation;
        this.synopsisID=synopsisID;
        this.estimationParams=estimationParams;
        this.StreamID=StreamID;
        this.DatasetKey=DatasetKey;
        this.field=field;
    }

    public String getEstimationVal() {
        return estimationVal;
    }

    public void setEstimationVal(String estimationVal) {
        this.estimationVal = estimationVal;
    }



    public int getSynopsisID() {
        return synopsisID;
    }

    public void setSynopsisID(int synopsisID) {
        this.synopsisID = synopsisID;
    }

    public Object[] getEstimationParams() {
        return estimationParams;
    }

    public void setEstimationParams(Object[] estimationParams) {
        this.estimationParams = estimationParams;
    }

    public String getStreamID() {
        return StreamID;
    }

    public void setStreamID(String streamID) {
        StreamID = streamID;
    }

    public String getDatasetKey() {
        return DatasetKey;
    }

    public void setDatasetKey(String datasetKey) {
        DatasetKey = datasetKey;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }
}
