package org.cloudbus.cloudsim.workflow;

import java.util.Comparator;
import java.util.Map.Entry;

import org.cloudbus.cloudsim.Vm;

/**
 * This class implements a comparator for different VM Offers.
 *
 */
public class OffersComparator implements Comparator<Entry<Vm, Integer>>{

	@Override
	public int compare(Entry<Vm, Integer> o1,Entry<Vm, Integer> o2) {
		
		if (o1.getValue()>o2.getValue()) return 1;
		if (o1.getValue()<o2.getValue()) return -1;
		
		return 0;
	}
}
