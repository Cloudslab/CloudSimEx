package org.cloudbus.cloudsim.ex.billing;

import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.ex.BaseDatacenterBrokerTest;
import org.cloudbus.cloudsim.ex.util.Id;
import org.cloudbus.cloudsim.ex.vm.VMex;

/**
 * 
 * @author nikolay.grozev
 * 
 */
public abstract class BaseBillingPolicyTest extends BaseDatacenterBrokerTest {

    /**
     * All policies use VmEx...
     */
    protected VMex createVM() {
	int pesNumber = 1; // number of cpus
	String vmm = "Xen"; // VMM name

	return new VMex(Id.pollId(Vm.class), broker.getId(), VM_MIPS, pesNumber,
		VM_RAM, VM_BW, VM_SIZE, vmm, new CloudletSchedulerTimeShared(), new String[3]);
    }
}
