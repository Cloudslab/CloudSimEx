package org.cloudbus.cloudsim.ex.billing;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.cloudbus.cloudsim.ex.vm.VMex;

/**
 * Implements a policy for billing a customer's vms. Simply sums the bills for
 * all VMs. <strong>NOTE:</strong> This implementation assumes that the metadata
 * of the VM is in the form [vm-type, OS], where vm-type is the type of the VM,
 * and OS is the operating system.
 * 
 * @author nikolay.grozev
 * 
 */
public abstract class BaseCustomerVmBillingPolicy implements IVmBillingPolicy<VMex> {

    protected final Map<Pair<String, String>, BigDecimal> prices;

    /**
     * Constr.
     * 
     * @param prices
     *            - the prices in a map with entries in the form [[vm-type, OS],
     *            price].It us up to the subclasses to interpret if these prices
     *            are per hour/minute etc.
     */
    public BaseCustomerVmBillingPolicy(final Map<Pair<String, String>, BigDecimal> prices) {
	this.prices = prices;
    }

    @Override
    public BigDecimal bill(final List<VMex> vms) {
	BigDecimal result = BigDecimal.ZERO;
	for (VMex vm : vms) {
	    if (shouldBillVm(vm)) {
		result = result.add(billSingleVm(vm));
	    }
	}
	return result;
    }

    /**
     * Returns if the VM should be billed. Override this method to implement
     * logic like billing the VMs of a given user.
     * 
     * @param vm
     *            - the vm to check for.
     * @return if the VM should be billed.
     */
    public boolean shouldBillVm(final VMex vm) {
	return keyOf(vm) != null;
    }

    /**
     * Returns the bill for a single VM.
     * 
     * @param vm
     *            - the vm
     * @return the bill for a single VM.
     */
    public abstract BigDecimal billSingleVm(final VMex vm);

    protected static ImmutablePair<String, String> keyOf(final VMex vm) {
	if (vm.getMetadata() != null && vm.getMetadata().length >= 2) {
	    return ImmutablePair.of(vm.getMetadata()[0], vm.getMetadata()[1]);
	}
	return null;
    }
}
