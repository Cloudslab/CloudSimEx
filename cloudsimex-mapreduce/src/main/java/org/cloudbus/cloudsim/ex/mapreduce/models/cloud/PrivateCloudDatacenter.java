package org.cloudbus.cloudsim.ex.mapreduce.models.cloud;

import java.util.List;

public class PrivateCloudDatacenter extends CloudDatacenter {
	
	public PrivateCloudDatacenter (String name, int hosts, int memory_perhost, int cores_perhost, int mips_precore_perhost, List<VmType> vmtypes) throws Exception {
		super(name, hosts, memory_perhost, cores_perhost, mips_precore_perhost, vmtypes);
	}

}
