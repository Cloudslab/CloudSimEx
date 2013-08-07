package org.cloudbus.cloudsim.ex.web;
import java.util.Collection;


/**
 * 
 */

/**
 * @author nikolay.grozev
 * 
 */
public class LocalisedWebSession extends WebSession {

    /**
     * Creates a new instance with the specified cloudlet generators.
     * 
     * @param appServerCloudLets
     *            - a generator for cloudlets for the application server. Must
     *            not be null.
     * @param dbServerCloudLets
     *            - a generator for cloudlets for the db server. Must not be
     *            null.
     * @param userId
     *            - the use id. A valid user id must be set either through a
     *            constructor or the set method, before this instance is used.
     */
    public LocalisedWebSession(IGenerator<? extends WebCloudlet> appServerCloudLets,
	    IGenerator<? extends Collection<? extends WebCloudlet>> dbServerCloudLets,
	    int userId,
	    int numberOfCloudlets,
	    double idealEnd) {
	super(appServerCloudLets, dbServerCloudLets, userId, numberOfCloudlets, idealEnd);
    }

}
