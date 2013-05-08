package org.cloudbus.cloudsim.ex.mapreduce.models.cloud;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.*;

import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.ex.mapreduce.VmSchedulerSpaceSharedMapReduce;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.UserClass;

public class PrivateCloudDatacenter extends CloudDatacenter {
	
	Map<UserClass, Double> userClassesReservationPercentage;
	
	public PrivateCloudDatacenter (String name, int hosts, int memory_perhost, int cores_perhost, int mips_precore_perhost, List<VmType> vmtypes) throws Exception {
		super(name, hosts, memory_perhost, cores_perhost, mips_precore_perhost, vmtypes);
	}

	public int getMaxAvailableResource(VmType firstVmType, UserClass userClass) {
		Double lowerThresholdPercentage = 100.0 - userClassesReservationPercentage.get(userClass);
		
		VmAllocationPolicy vmAllocationPolicy = getVmAllocationPolicy();
		List<Host> hostList = vmAllocationPolicy.getHostList();
		int maxAvailableResource = 0;
		int totalOfResources = 0;
		for (Host host : hostList) {
			int numberOfHostTotalPes = host.getNumberOfPes();
			
			int numberOfVmPes = firstVmType.getNumberOfPes();
			VmSchedulerSpaceSharedMapReduce vmScheduler = (VmSchedulerSpaceSharedMapReduce) host.getVmScheduler();
			int numberOfHostFreePes = vmScheduler.getFreePes().size();
			maxAvailableResource += Math.floor((double)numberOfHostFreePes / numberOfVmPes);
			
			totalOfResources += Math.floor((double)numberOfHostTotalPes / numberOfVmPes);
		}
		
		double maxAvailableResourcePercentage = ((double)maxAvailableResource/totalOfResources) * 100;
		
		if(maxAvailableResourcePercentage >= lowerThresholdPercentage)
			return maxAvailableResource;
		else
			return 0;
	}

	public Map<UserClass, Double> getUserClassesReservationPercentage()
	{
		return userClassesReservationPercentage;
	}

	public void setUserClassesReservationPercentage(Map<UserClass, Double> userClassesReservationPercentage)
	{
		this.userClassesReservationPercentage = userClassesReservationPercentage;
	}

}
