package org.kafkaApp.Structure;


import org.kafkaApp.Synopses.Synopsis;

public class SynopsisAndParameters {
    private Synopsis synopsis;
    private Object[] parameters;
    private int countReqProc;
    public SynopsisAndParameters(){}
    public SynopsisAndParameters(Synopsis synopsis ,Object[] parameters,int countReqProc){
        this.synopsis=synopsis;
        this.parameters=parameters;
        this.countReqProc=countReqProc;
    }


    public Synopsis getSynopsis() {
        return synopsis;
    }

    public void setSynopsis(Synopsis synopsis) {
        this.synopsis = synopsis;
    }

    public Object[] getParameters() {
        return parameters;
    }

    public void setParameters(Object[] parameters) {
        this.parameters = parameters;
    }

    public int getCountReqProc() {
        return countReqProc;
    }

    public void setCountReqProc(int countReqProc) {
        this.countReqProc = countReqProc;
    }
}
