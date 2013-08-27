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
public interface IVmBillingPolicy<V extends Vm> {

    /** Constant for *nix OS-es. */
    public static final String LINUX = "Linux/Unix";
    /** Constant for Windows OS-es. */
    public static final String WINDOWS = "Windows";
    /** One minute time. */
    public static int MINUTE = 60;
    /** One hour time. */
    public static int HOUR = 60 * MINUTE;
    /** One day time. */
    public static int DAY = 24 * HOUR;

    /**
     * Returns the cost for the specified vms.
     * 
     * @param vms
     *            - the vms to bill. Must not be empty
     * @return the cost for the specified vms.
     */
    public BigDecimal bill(final List<V> vms);

}
