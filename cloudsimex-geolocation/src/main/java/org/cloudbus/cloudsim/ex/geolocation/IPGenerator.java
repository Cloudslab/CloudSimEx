package org.cloudbus.cloudsim.ex.geolocation;

import java.util.Set;

/**
 * Generates random IPs from specified countries.
 * 
 * @author nikolay.grozev
 * 
 */
public interface IPGenerator {

    /**
     * Returns the ISO codes of the countries for which this generator generates
     * IPs.
     * 
     * @return the ISO codes of the countries for which this generator generates
     *         IPs.
     */
    public abstract Set<String> getCountryCodes();

    /**
     * Creates a random IP from the specified countries. This may be either IPv4
     * or IPv6. The returned values is in the standard dot notation. If no IP
     * could be generated, null is returned.
     * 
     * @return a random IP from the specified countries or null, if no value
     *         could be generated.
     */
    public abstract String pollRandomIP();

}