package org.cloudbus.cloudsim.workflow;

import java.util.Comparator;

public class SlotComparator implements Comparator<TimeSlot> {

	@Override
	public int compare(TimeSlot slot0, TimeSlot slot1) {
		//first criteria: we favour paid slots
		if (slot0.isAlreadyPaid() && !slot1.isAlreadyPaid()) return 1;
		if (!slot0.isAlreadyPaid() && slot1.isAlreadyPaid()) return -1;
		
		//if both are paid or unpaid, the winner is the biggest
		long size0=slot0.getEndTime()-slot0.getStartTime();
		long size1=slot1.getEndTime()-slot1.getStartTime();
		
		if (size0>size1) return 1;
		if (size0<size1) return -1;
		
		return 0;
	}
}
