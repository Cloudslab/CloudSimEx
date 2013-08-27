package org.cloudbus.cloudsim.ex.billing;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
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
	BigDecimal pricePerMin = prices.get(keyOf(vm)).divide(BigDecimal.valueOf(60), RoundingMode.HALF_UP);
	
	double timeAfterBoot = vm.getTimeAfterBooting();
	int chargeCount = (int) timeAfterBoot / MINUTE + 1;
	if (timeAfterBoot == (int) timeAfterBoot && (int) timeAfterBoot % MINUTE == 0) {
	    chargeCount = (int) timeAfterBoot / MINUTE;
	}

	chargeCount = Math.min(10, chargeCount);
	return pricePerMin.multiply(BigDecimal.valueOf(chargeCount));
    }
}
