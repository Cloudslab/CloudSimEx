package org.cloudbus.cloudsim.ex.billing;

import java.math.BigDecimal;
import java.util.List;

import org.cloudbus.cloudsim.Vm;

/**
 * Defines the billing policy of a cloud provider with respect to pricing VMs.
 * <strong>NOTE!</strong> - the implementations of this interface are completely
 * independent from the pricing properties defined in the data centre objects.
 * 
 * @author nikolay.grozev
 * 
 */
public interface IVmBillingPolicy {

    /**
     * Returns the cost for the specified vms.
     * 
     * @param vms
     *            - the vms to bill. Must not be empty
     * @return the cost for the specified vms.
     */
    public BigDecimal bill(final List<Vm> vms);

    /**
     * Returns the next charging time.
     * 
     * @return the next charging time or -1 if the charge time could not be
     *         estimated.
     */
    public double nexChargeTime(final Vm vm);

}
