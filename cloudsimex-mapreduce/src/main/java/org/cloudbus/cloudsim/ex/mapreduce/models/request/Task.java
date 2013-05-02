package org.cloudbus.cloudsim.ex.mapreduce.models.request;

import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.ex.mapreduce.MapReduceEngine;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VmInstance;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.VmType;
import org.cloudbus.cloudsim.ex.util.Id;

public class Task extends Cloudlet {

	public String name;
	public int requestId;
	public boolean isFinished;
	public int mi;
	public int dSize;
	
	public Task(String name, int dSize, int mi) {
		//Cloudlet lengh is 0, it will be updated after we run the algorithm
		//File size is 0, we will use the new dSize
		super(Id.pollId(Task.class), 0, 1, 0, 0, new UtilizationModelFull(), new UtilizationModelFull(), new UtilizationModelFull());
		
		this.name = name;
		requestId = -1;
		isFinished = false;
		this.mi = mi;
		this.dSize = dSize;
		
		this.setUserId(Cloud.brokerID);
	}
	
	protected Cloud getCloud()
	{
		return getMapReduceEngine().getCloud();
	}
	
	protected Requests getRequests()
	{
		return getMapReduceEngine().getRequests();
	}
	
	public Request getCurrentRequest()
	{
		return getRequests().getRequestFromId(requestId);
	}
	
	protected VmInstance getCurrentVmInstance()
	{
		//Check if it has been binded
		if(getVmId() != -1)
			return getCurrentRequest().getProvisionedVm(getVmId());
		else
			return getCurrentRequest().getProvisionedVmFromTaskId(getCloudletId());
	}
	
	protected VmType getCurrentVmType()
	{
		return getCloud().getVMTypeFromId(getCurrentVmInstance().VmTypeId);
	}
	
	private MapReduceEngine getMapReduceEngine()
	{
		return (MapReduceEngine) CloudSim.getEntity("MapReduceEngine");
	}
	
	/**
	 * To tell the vm how long it will run, we need to convert the time to MI (Million Instructions)
	 * @return
	 */
	public double getTaskExecutionTimeInMillionInstructions()
	{
		return mi;
	}
	
	public double getTaskExecutionTimeInSeconds()
	{
		VmInstance vm = getCurrentVmInstance();
		return mi / vm.getMips();
	}
	
	
	public void updateCloudletLength()
	{
		setCloudletLength((long) getTaskExecutionTimeInMillionInstructions());
	}
}
