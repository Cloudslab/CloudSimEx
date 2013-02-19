package org.cloudbus.cloudsim.mapreduce.models.cloud;

import org.cloudbus.cloudsim.CloudletScheduler;
import org.cloudbus.cloudsim.Vm;

public class VMType extends Vm {

	public VMType(int id, int userId, double mips, int numberOfPes, int ram,
			long bw, long size, String vmm, CloudletScheduler cloudletScheduler) {
		super(id, userId, mips, numberOfPes, ram, bw, size, vmm, cloudletScheduler);
		// TODO Auto-generated constructor stub
	}

}
