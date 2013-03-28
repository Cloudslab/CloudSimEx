package org.cloudbus.cloudsim.ex.mapreduce.models.cloud;

import java.util.ArrayList;
import java.util.List;
import org.cloudbus.cloudsim.*;

import org.cloudbus.cloudsim.DatacenterCharacteristics;

public class PrivateCloudDatacenter extends CloudDatacenter {
	
	public PrivateCloudDatacenter (String name, int hosts, int memory_perhost, int cores_perhost, int mips_precore_perhost, List<VmType> vmtypes) throws Exception {
		super(name, hosts, memory_perhost, cores_perhost, mips_precore_perhost, vmtypes);
	}

	public int getMaxAvailableResource(VmType firstVmType) {
		DatacenterCharacteristics datacenterCharacteristics = getCharacteristics();
		List<Host> hostList = datacenterCharacteristics.getHostList();
		int maxAvailableResource = 0;
		for (Host host : hostList) {
			int numberOfVmPes = firstVmType.getNumberOfPes();
			int numberOfFreePes = host.getNumberOfFreePes();
			
			maxAvailableResource += Math.floor((double)numberOfFreePes / numberOfVmPes);
		}
		
		return maxAvailableResource;
	}

}
