package org.cloudbus.cloudsim.ex.mapreduce.models.cloud;

import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.ex.mapreduce.MapReduceCloudletScheduler;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Request;
import org.cloudbus.cloudsim.ex.util.Id;

public class VmType extends Vm {

    public String name;
    public double vmCostPerHour;
    public double transferringCost;
    public int bootTime;

    public VmType(String name, Double vmCostPerHour, Double transferringCost, Double mips, int cores, int ram,
	    int bootTime) {
	super(Id.pollId(VmType.class), Cloud.brokerID, mips, cores, ram, 1000, 0, "Xen",
		new MapReduceCloudletScheduler());

	this.name = name;
	this.vmCostPerHour = vmCostPerHour;
	this.transferringCost = transferringCost;
	this.bootTime = bootTime;
    }

    public VmInstance getVmInstance(Request request) {
	return new VmInstance(this, request);
    }

    public String getName() {
	return name;
    }

    public void setName(String name) {
	this.name = name;
    }

    public double getVmCostPerHour()
    {
	return vmCostPerHour;
    }

    public void setVmCostPerHour(double vmCostPerHour) {
	this.vmCostPerHour = vmCostPerHour;
    }

    public double getTransferringCost() {
	return transferringCost;
    }

    public void setTransferringCost(double transferringCost) {
	this.transferringCost = transferringCost;
    }

    public int getBootTime() {
	return bootTime;
    }

    public void setBootTime(int bootTime) {
	this.bootTime = bootTime;
    }

}
