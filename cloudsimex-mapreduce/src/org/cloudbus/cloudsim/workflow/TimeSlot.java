package org.cloudbus.cloudsim.workflow;

import org.cloudbus.cloudsim.Vm;

public class TimeSlot {
	
	Vm vm;
	long startTime;
	long endTime;
	boolean alreadyPaid;
	
	public TimeSlot(Vm vm, long startTime, long endTime, boolean alreadyPaid) {
		this.vm = vm;
		this.startTime = startTime;
		this.endTime = endTime;
		this.alreadyPaid = alreadyPaid;
	}
	
	public Vm getVm(){
		return vm;
	}
	
	public long getStartTime() {
		return startTime;
	}
	
	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}
	
	public long getEndTime() {
		return endTime;
	}
	
	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}
	
	public void setAlreadyPaid(boolean alreadyPaid){
		this.alreadyPaid = alreadyPaid;
	}
	
	public boolean isAlreadyPaid() {
		return alreadyPaid;
	}
}
