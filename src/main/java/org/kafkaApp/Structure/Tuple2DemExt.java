package org.kafkaApp.Structure;

public class Tuple2DemExt <A, B> extends Tuple2Dem<A, B>{
    private String isProcessed;

    public Tuple2DemExt() {
        super();
    }

    public Tuple2DemExt(A value1, B value2, String isProcessed) {
        super(value1, value2);
        this.isProcessed = isProcessed;
    }

    public String getIsProcessed() {
        return isProcessed;
    }

    public void setIsProcessed(String isProcessed) {
        this.isProcessed = isProcessed;
    }
}
