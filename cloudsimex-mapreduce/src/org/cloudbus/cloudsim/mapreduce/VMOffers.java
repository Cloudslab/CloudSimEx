package org.cloudbus.cloudsim.mapreduce;

import java.util.Hashtable;

import org.cloudbus.cloudsim.Vm;

/**
 * This abstract contains encapsulates VM options offered
 * by IaaS providers. Its methods return instance types offered,
 * cost of different instances and the leasing time slot.
 *
 */
public abstract class VMOffers {
	
	Hashtable<Vm,Integer> vmOffersTable;
	
	/**
	 * Returns the instances offered by the provider, and the respective
	 * prices per time slot (in cents).
	 */
	public abstract Hashtable<Vm,Integer> getVmOffers();
	
	/**
	 * Returns the cost of a specific instance, per time slot, in cents.
	 */
	public abstract int getCost(double mips,int memory,long bw);
	
	/**
	 * Returns the duration of the lease time slot in seconds.
	 */
	public abstract long getTimeSlot();
	
	/**
	 * Returns the average boot time of a VM in seconds.
	 */
	public abstract long getBootTime();

	public VMOffers(){
		vmOffersTable = new Hashtable<Vm,Integer>();
	}
}
