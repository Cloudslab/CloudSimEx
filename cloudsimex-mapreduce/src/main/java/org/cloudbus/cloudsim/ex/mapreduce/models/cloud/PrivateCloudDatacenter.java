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

    public PrivateCloudDatacenter(String name, int hosts, int memory_perhost, int cores_perhost,
	    int mips_precore_perhost, List<VmType> vmtypes) throws Exception {
	super(name, hosts, memory_perhost, cores_perhost, mips_precore_perhost, vmtypes);
    }

    public int getMaxAvailableResource(VmType firstVmType, UserClass userClass) {
	Double allowedPercentage = userClassesReservationPercentage.get(userClass);

	VmAllocationPolicy vmAllocationPolicy = getVmAllocationPolicy();
	List<Host> hostList = vmAllocationPolicy.getHostList();
	int availableResource = 0;
	int totalResources = 0;
	for (Host host : hostList) {
	    int numberOfHostTotalPes = host.getNumberOfPes();

	    int numberOfVmPes = firstVmType.getNumberOfPes();
	    VmSchedulerSpaceSharedMapReduce vmScheduler = (VmSchedulerSpaceSharedMapReduce) host.getVmScheduler();
	    int numberOfHostFreePes = vmScheduler.getFreePes().size();
	    availableResource += Math.floor((double) numberOfHostFreePes / numberOfVmPes);

	    totalResources += Math.floor((double) numberOfHostTotalPes / numberOfVmPes);
	}
	
	int allowedResources = (int) Math.floor(totalResources * (allowedPercentage / 100.0));
	int usedResources = totalResources - availableResource;
	return allowedResources-usedResources;
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
