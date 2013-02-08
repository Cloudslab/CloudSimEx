package org.cloudbus.cloudsim.ex.disk;

import java.util.Arrays;
import java.util.List;

import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.VmScheduler;
import org.cloudbus.cloudsim.VmSchedulerTimeSharedOverSubscription;
import org.cloudbus.cloudsim.ex.VmSchedulerWithIndependentPes;

/**
 * 
 * Schedules harddisks between VMs on a host.
 * 
 * @author nikolay.grozev
 * 
 */
public class VmDiskScheduler extends VmSchedulerWithIndependentPes<HddPe, HddVm> {

    public VmDiskScheduler(final List<HddPe> pelist) {
	super(pelist);
    }

    @Override
    protected VmScheduler createSchedulerFroPe(final HddPe pe) {
	return new VmSchedulerTimeSharedOverSubscription(Arrays.asList(pe));
    }

    @Override
    protected boolean doesVmUse(final HddVm vm, final Pe pe) {
	return vm.getHddsIds().contains(pe.getId());
    }

}
