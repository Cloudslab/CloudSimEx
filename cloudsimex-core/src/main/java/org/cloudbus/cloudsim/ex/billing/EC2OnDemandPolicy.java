package org.cloudbus.cloudsim.ex.billing;

import java.math.BigDecimal;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.cloudbus.cloudsim.ex.vm.VMex;

/**
 * Implements the default billing policy for Amazon EC2 on demand instances.
 * 
 * 
 * @author nikolay.grozev
 * 
 */
public class EC2OnDemandPolicy extends BaseCustomerVmBillingPolicy {

    /**
     * Constr.
     * 
     * @param prices
     *            - the prices in a map with entries in the form [[vm-type, OS],
     *            price]. Prices are hourly.
     */
    public EC2OnDemandPolicy(final Map<Pair<String, String>, BigDecimal> prices) {
	super(prices);
    }

    @Override
    public BigDecimal billSingleVm(final VMex vm) {
	double timeAfterBoot = vm.getTimeAfterBooting();
	int chargeCount = (int) timeAfterBoot / HOUR + 1;
	if(timeAfterBoot == (int)timeAfterBoot && (int)timeAfterBoot % HOUR ==0 ) {
	    chargeCount = (int) timeAfterBoot / HOUR;
	}
	
	return prices.get(keyOf(vm)).multiply(BigDecimal.valueOf(chargeCount));
    }
}
