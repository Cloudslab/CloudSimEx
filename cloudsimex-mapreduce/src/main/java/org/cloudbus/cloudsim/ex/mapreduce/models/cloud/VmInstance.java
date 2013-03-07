package org.cloudbus.cloudsim.ex.mapreduce.models.cloud;

public class VmInstance extends VmType {

	public int VmTypeId;
	
	public VmInstance(VmType vmType) {
		super(vmType.name, vmType.cost, vmType.transferringCost, vmType.getMips(), vmType.getNumberOfPes(), vmType.getRam(), vmType.bootTime);
		
		VmTypeId = vmType.getId();
	}

}
