package org.kafkaApp.Structure;

public class InstanceStateStructure {
    private String instanceNameId;
    private int instanceTotalCount;
    private String rebalanceStateStatus;
    private int numOfReplica;

    public InstanceStateStructure() {

    }
    public InstanceStateStructure(String instanceNameId, int instanceTotalCount, String rebalanceStateStatus, int numOfReplica) {
    	this.instanceNameId = instanceNameId;
    	this.instanceTotalCount = instanceTotalCount;
    	this.rebalanceStateStatus = rebalanceStateStatus;
    	this.numOfReplica = numOfReplica;
    }


    public String getInstanceNameId() {
        return instanceNameId;
    }

    public void setInstanceNameId(String instanceNameId) {
        this.instanceNameId = instanceNameId;
    }

    public int getInstanceTotalCount() {
        return instanceTotalCount;
    }

    public void setInstanceTotalCount(int instanceTotalCount) {
        this.instanceTotalCount = instanceTotalCount;
    }

    public String getRebalanceStateStatus() {
        return rebalanceStateStatus;
    }

    public void setRebalanceStateStatus(String rebalanceStateStatus) {
        this.rebalanceStateStatus = rebalanceStateStatus;
    }

    public int getNumOfReplica() {
        return numOfReplica;
    }

    public void setNumOfReplica(int numOfReplica) {
        this.numOfReplica = numOfReplica;
    }
}
