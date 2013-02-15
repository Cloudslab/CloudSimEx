package org.cloudbus.cloudsim.workflow;

import java.util.Comparator;

/**
 * Implements a comparator for cost of virtual machines
 */
public class CostComparator implements Comparator<ProvisionedVm> {
	
	@Override
	public int compare(ProvisionedVm vm0, ProvisionedVm vm1) {
		if (vm0.getCost()>vm1.getCost()) return 1;
		if (vm0.getCost()<vm1.getCost()) return -1;
		return 0;
	}
}
