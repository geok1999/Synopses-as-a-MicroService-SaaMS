package org.kafkaApp.Structure;


import org.kafkaApp.Synopses.Synopsis;

public class SynopsisAndRequest {
    private Synopsis synopsis;
    private RequestStructure request;

    public SynopsisAndRequest(){}
    public SynopsisAndRequest(Synopsis synopsis ,  RequestStructure request){
        this.synopsis=synopsis;
        this.request=request;

    }


    public Synopsis getSynopsis() {
        return synopsis;
    }

    public void setSynopsis(Synopsis synopsis) {
        this.synopsis = synopsis;
    }


    public RequestStructure getRequest() {
        return request;
    }

    public void setRequest(RequestStructure request) {
        this.request = request;
    }
}
