package org.cloudbus.cloudsim.ex.mapreduce;

import java.util.Hashtable;

import org.cloudbus.cloudsim.Vm;

public class VmOffersSimple extends VMOffers {
	
	int vmsCount;
	int baseCore;
	long baseSize;
	int baseMem;
	
	
	@Override
	public Hashtable<Vm, Double> getVmOffers() {
		vmsCount = Integer.parseInt(Properties.VMS_COUNT.getProperty());
		
		baseCore = Integer.parseInt(Properties.CORE_PERVM.getProperty());
		baseSize = Long.parseLong(Properties.SIZE_PERVM.getProperty());
		baseMem = Integer.parseInt(Properties.MEMORY_PERVM.getProperty());
		
		
		for (int i=1; i<=vmsCount; i++) {
			int mips = Integer.parseInt(Properties.VM1_MIPS.getProperty());
			double cost = Double.parseDouble(Properties.VM1_COST.getProperty());
			//WARNING: Missing transferring.cost, ar, and datacenter
			//WARNING: bandwidth is 100000, is that OK?
			vmOffersTable.put(new Vm(0,0,mips,baseCore,  baseMem,10000,  baseSize,"",null),   cost);
		}
		
		return vmOffersTable;
	}

	@Override
	public long getTimeSlot() {
		return 3600; //one hour, in seconds
	}

	@Override
	public long getBootTime() {
		return Long.parseLong(Properties.VM_DELAY.getProperty());
	}
}
