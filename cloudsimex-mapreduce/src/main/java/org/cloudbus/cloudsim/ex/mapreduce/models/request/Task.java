package org.cloudbus.cloudsim.ex.mapreduce.models.request;

import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.ex.mapreduce.MapReduceEngine;
import org.cloudbus.cloudsim.ex.mapreduce.models.cloud.Cloud;
import org.cloudbus.cloudsim.ex.util.Id;

public class Task extends Cloudlet {

	public String description;
	public String name;
	public int requestId;
	
	public Task(String name, String description, int dSize, int mipb) {
		//WARNING: pesNumber is 1, is that OK?
		super(Id.pollId(Task.class), mipb, 1, dSize, 0, new UtilizationModelFull(), new UtilizationModelFull(), new UtilizationModelFull());
		
		this.name = name;
		this.description = description;
		requestId = -1;
		
		this.setUserId(Cloud.brokerID);
	}
}
