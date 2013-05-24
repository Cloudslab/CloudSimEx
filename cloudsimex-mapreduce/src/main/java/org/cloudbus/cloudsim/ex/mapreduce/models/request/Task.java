package org.cloudbus.cloudsim.ex.mapreduce.models.request;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.ex.mapreduce.MapReduceEngine;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VmInstance;
import org.cloudbus.cloudsim.ex.mapreduce.policy.Policy.CloudDeploymentModel;
import org.cloudbus.cloudsim.ex.util.Id;

public class Task extends Cloudlet {

    public String name;
    public int requestId;
    public boolean isFinished;
    public int mi;
    public int dSize;
    public MapReduceEngine mapReduceEngine;

    private int experimentNumber;
    private int workloadNumber;

    public Task(String name, int dSize, int mi) {
	// Cloudlet lengh is 0, it will be updated after we run the algorithm
	// File size is 0, we will use the new dSize
	super(Id.pollId(Task.class), 0, 1, 0, 0, new UtilizationModelFull(), new UtilizationModelFull(),
		new UtilizationModelFull());

	this.name = name;
	requestId = -1;
	isFinished = false;
	this.mi = mi;
	this.dSize = dSize;
	
	

	this.setUserId(Cloud.brokerID);
    }

    public Cloud getCloud()
    {
	return mapReduceEngine.getCloud();
    }

    public Requests getRequests()
    {
	return mapReduceEngine.getRequests();
    }

    public Request getCurrentRequest()
    {
    	return getRequests().getRequestFromId(requestId);
    }

    public VmInstance getCurrentVmInstance()
    {
	// Check if it has been binded
	if (getVmId() != -1)
	    return getCurrentRequest().getProvisionedVm(getVmId());
	else
	    return getCurrentRequest().getProvisionedVmFromTaskId(getCloudletId());
    }

    public MapReduceEngine getMapReduceEngine()
    {
	return mapReduceEngine;
    }

    public double getTaskExecutionTimeInSeconds()
    {
	return mi / getCurrentVmInstance().getMips();
    }

    public void updateCloudletLength()
    {
	setCloudletLength((long) mi);
    }

    public String getName() {
	return name;
    }

    public void setName(String name) {
	this.name = name;
    }

    public int getRequestId() {
	return requestId;
    }

    public void setRequestId(int requestId) {
	this.requestId = requestId;
    }

    public boolean isFinished() {
	return isFinished;
    }

    public void setFinished(boolean isFinished) {
	this.isFinished = isFinished;
    }

    public int getMi() {
	return mi;
    }

    public void setMi(int mi) {
	this.mi = mi;
    }

    public int getdSize() {
	return dSize;
    }

    public void setdSize(int dSize) {
	this.dSize = dSize;
    }

    public double getFinalExecTime() {
	return getFinishTime() - getExecStartTime();
    }

    public String getTaskType()
    {
	// MUST OVERRIDE IT
	return "Something is wrong .. you should get Map or Reduce";
    }

    public int getInstanceVmId()
    {
	return getVmId();
    }

    public String getVmType()
    {
	return getRequests().getVmInstance(getVmId()).name;
    }

    public int getExperimentNumber()
    {
	return experimentNumber;
    }

    public void setExperimentNumber(int experimentNumber)
    {
	this.experimentNumber = experimentNumber;
    }
    
    public int getWorkloadNumber()
    {
	return workloadNumber;
    }

    public void setWorkloadNumber(int workloadNumber)
    {
	this.workloadNumber = workloadNumber;
    }

    public String getPolicy()
    {
	return getCurrentRequest().getPolicy();
    }

    public String getJ()
    {
	return getCurrentRequest().getJ();
    }

    public UserClass getUserClass()
    {
	return getCurrentRequest().getUserClass();
    }

    public CloudDeploymentModel getCloudDeploymentModel()
    {
	return getCurrentRequest().getCloudDeploymentModel();
    }
}
