package org.cloudbus.cloudsim.ex.mapreduce.models.cloud;

import java.util.ArrayList;
import java.util.List;
import org.cloudbus.cloudsim.*;

import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.ex.mapreduce.VmSchedulerSpaceSharedMapReduce;

public class PrivateCloudDatacenter extends CloudDatacenter {
	
	public PrivateCloudDatacenter (String name, int hosts, int memory_perhost, int cores_perhost, int mips_precore_perhost, List<VmType> vmtypes) throws Exception {
		super(name, hosts, memory_perhost, cores_perhost, mips_precore_perhost, vmtypes);
	}

	public int getMaxAvailableResource(VmType firstVmType) {
		VmAllocationPolicy vmAllocationPolicy = getVmAllocationPolicy();
		List<Host> hostList = vmAllocationPolicy.getHostList();
		int maxAvailableResource = 0;
		for (Host host : hostList) {
			int numberOfVmPes = firstVmType.getNumberOfPes();
			
			VmSchedulerSpaceSharedMapReduce vmScheduler = (VmSchedulerSpaceSharedMapReduce) host.getVmScheduler();
			int numberOfFreePes = vmScheduler.getFreePes().size();
			
			maxAvailableResource += Math.floor((double)numberOfFreePes / numberOfVmPes);
		}
		
		return maxAvailableResource;
	}

}
