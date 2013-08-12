package org.cloudbus.cloudsim.ex.geolocation;

import java.util.Collections;
import java.util.Random;
import java.util.Set;

/**
 * 
 * Implements common functionalities for random {@link IPGenerator} implementations.
 * 
 * @author nikolay.grozev
 *
 */
public abstract class BaseIPGenerator implements IPGenerator {

    private final Set<String> countryCodes;
    private final Random random = new Random();

    /**
     * Constr.
     * @param countryCodes - the country codes for this generator.
     */
    public BaseIPGenerator(final Set<String> countryCodes) {
	this.countryCodes = Collections.unmodifiableSet(countryCodes);
    }

    /**
     * Constr.
     * @param countryCodes - the country codes for this generator.
     * @param seed - a seed if we need to get the same behavior again and again.
     */
    public BaseIPGenerator(final Set<String> countryCodes, final long seed) {
	this.countryCodes = Collections.unmodifiableSet(countryCodes);
	random.setSeed(seed);
    }
    
    @Override
    public Set<String> getCountryCodes() {
	return countryCodes;
    }

    /**
     * Returns a random object to be used to get the same behavior again and again.
     * @return the random object.
     */
    protected Random getRandom() {
        return random;
    }
}