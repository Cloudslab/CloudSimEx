package org.cloudbus.cloudsim.ex.mapreduce.models.cloud;

import org.cloudbus.cloudsim.CloudletScheduler;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.ex.mapreduce.models.Cloud;
import org.cloudbus.cloudsim.ex.util.*;

public class VMType extends Vm {

	public String name;
	public Double cost;
	public Double transferringCost;
	public int ar;
	public int bootTime;
	
	

	
	public VMType(String name, Double cost, Double transferringCost, int mips, int ar, int cores, int ram, int bootTime) {
		//WARNING: bw is 1000, is that OK?
		//WARNING: userId (Broker ID) is Cloud.brokerID, is that Ok?
		//WARNING: Size is 0, is that OK?
		super(Id.pollId(VMType.class), Cloud.brokerID, mips, cores, ram, 1000, 0, "Xen", new CloudletSchedulerTimeShared());
		
		this.name = name;
		this.cost = cost;
		this.transferringCost = transferringCost;
		this.ar = ar;
		this.bootTime = bootTime;
	}



	public VMType(int id, int userId, double mips, int numberOfPes, int ram,
			long bw, long size, String vmm, CloudletScheduler cloudletScheduler) {
		super(id, userId, mips, numberOfPes, ram, bw, size, vmm, cloudletScheduler);
		// TODO Auto-generated constructor stub
	}

}
