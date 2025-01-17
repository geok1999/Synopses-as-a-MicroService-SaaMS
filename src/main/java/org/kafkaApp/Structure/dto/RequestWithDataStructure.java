package org.kafkaApp.Structure.dto;


import org.kafkaApp.Structure.entities.DataStructure;
import org.kafkaApp.Structure.entities.RequestStructure;

public class RequestWithDataStructure{
    private RequestStructure requestWithSynopsesStructure;
    private DataStructure data; //this can be a class or int with the value of the data we want to do estimate

    private int totalRunOfReq;

    public RequestWithDataStructure() {

    }
    public RequestWithDataStructure(RequestStructure requestWithSynopsesStructure, DataStructure data, int totalRunOfReq){
        this.data=data;
        this.requestWithSynopsesStructure=requestWithSynopsesStructure;
        this.totalRunOfReq = totalRunOfReq;
    }





    public RequestStructure getRequestWithSynopsesStructure() {
        return requestWithSynopsesStructure;
    }

    public void setRequestWithSynopsesStructure(RequestStructure requestWithSynopsesStructure) {
        this.requestWithSynopsesStructure = requestWithSynopsesStructure;
    }

    public int getTotalRunOfReq() {
        return totalRunOfReq;
    }

    public void setTotalRunOfReq(int totalRunOfReq) {
        this.totalRunOfReq = totalRunOfReq;
    }

    public DataStructure getData() {
        return data;
    }

    public void setData(DataStructure data) {
        this.data = data;
    }
}