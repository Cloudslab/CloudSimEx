package org.cloudbus.cloudsim.ex.mapreduce.models.cloud;

import org.cloudbus.cloudsim.ex.mapreduce.*;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.ex.util.*;

public class VMType extends Vm {

	public String name;
	public double cost;
	public double transferringCost;
	public int bootTime;
	public int ExecutionTime;
	

	
	public VMType(String name, Double cost, Double transferringCost, int mips, int cores, int ram, int bootTime) {
		//WARNING: bw is 1000, is that OK?
		//WARNING: userId (Broker ID) is Cloud.brokerID, is that Ok?
		//WARNING: Size is 0, is that OK?
		super(Id.pollId(VMType.class), Cloud.brokerID, mips, cores, ram, 1000, 0, "Xen", new MapReduceCloudletScheduler());
		
		this.name = name;
		this.cost = cost;
		this.transferringCost = transferringCost;
		this.bootTime = bootTime;
		ExecutionTime = 0;
	}

}
