package org.cloudbus.cloudsim.ex.mapreduce.models.cloud;

import java.util.List;

public class PublicCloudDatacenter extends CloudDatacenter {
	
	public PublicCloudDatacenter (String name, List<VMType> vmtypes) throws Exception {
		super(name, 200, 32768, 100, 99999, vmtypes);
	}

}
