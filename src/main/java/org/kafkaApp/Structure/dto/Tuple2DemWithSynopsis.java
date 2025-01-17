package org.kafkaApp.Structure.dto;

import org.kafkaApp.Synopses.Synopsis;

public class Tuple2DemWithSynopsis <A, B> extends Tuple2Dem<A, B>{
    private Synopsis synopsis;
    private int countReqProc;

    public Tuple2DemWithSynopsis(){
        super();
    }
    public Tuple2DemWithSynopsis(A value1,B value2, Synopsis synopsis,int countReqProc){
        super(value1, value2);
        this.synopsis = synopsis;
        this.countReqProc=countReqProc;
    }


    public int getCountReqProc() {
        return countReqProc;
    }

    public void setCountReqProc(int countReqProc) {
        this.countReqProc = countReqProc;
    }

    public Synopsis getSynopsis() {
        return synopsis;
    }

    public void setSynopsis(Synopsis synopsis) {
        this.synopsis = synopsis;
    }
}
