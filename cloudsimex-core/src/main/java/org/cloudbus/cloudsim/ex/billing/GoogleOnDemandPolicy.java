package org.cloudbus.cloudsim.ex.billing;

import static org.cloudbus.cloudsim.Consts.MINUTE;

import java.math.BigDecimal;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.ex.vm.VMStatus;
import org.cloudbus.cloudsim.ex.vm.VMex;

/**
 * Implements the default billing policy for the Google IaaS on demand
 * instances.
 * 
 * @author nikolay.grozev
 * 
 */
public class GoogleOnDemandPolicy extends BaseCustomerVmBillingPolicy {

    /**
     * Constr.
     * 
     * @param prices
     *            - the prices in a map with entries in the form [[vm-type, OS],
     *            price]. Prices are per hour.
     */
    public GoogleOnDemandPolicy(final Map<Pair<String, String>, BigDecimal> prices) {
	super(prices);
    }

    @Override
    public BigDecimal billSingleVm(final VMex vm) {
	BigDecimal pricePerMin = null;
	try {
	    pricePerMin = prices.get(keyOf(vm)).divide(BigDecimal.valueOf(60));
	} catch (ArithmeticException ex) {
	    pricePerMin = BigDecimal.valueOf(prices.get(keyOf(vm)).doubleValue() / 60);
	}

	double timeAfterBoot = vm.getTimeAfterBooting();
	int chargeCount = (int) timeAfterBoot / MINUTE + 1;
	if (timeAfterBoot == (int) timeAfterBoot && (int) timeAfterBoot % MINUTE == 0) {
	    chargeCount = (int) timeAfterBoot / MINUTE;
	}

	chargeCount = Math.max(10, chargeCount);
	return pricePerMin.multiply(BigDecimal.valueOf(chargeCount));
    }

    @Override
    public double nexChargeTime(final Vm vm) {
	double result = -1;
	if (vm instanceof VMex && ((VMex) vm).getStatus() == VMStatus.RUNNING) {
	    VMex vmex = (VMex) vm;
	    double elapsedTime = getCurrentTime() - vmex.getStartTime();
	    result = vmex.getStartTime() + MINUTE * Math.max(10, (int) (elapsedTime / MINUTE) + 1);
	}
	return result;
    }

}
