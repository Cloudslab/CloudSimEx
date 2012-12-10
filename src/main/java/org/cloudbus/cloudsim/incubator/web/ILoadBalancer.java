package org.cloudbus.cloudsim.incubator.web;

import java.util.List;

import org.cloudbus.cloudsim.incubator.web.extensions.HddVm;

/**
 * The load balancer of an application. Assigns sessions to servers.
 * 
 * @author nikolay.grozev
 * 
 */
public interface ILoadBalancer {

    /**
     * The id of the load balancer.
     * 
     * @return - the id of the load balancer.
     */
    public long getId();

    /**
     * Assigns the specified sessions to an applicaiton and a DB servers.
     * 
     * @param sessions
     *            - the sessions to assign. If the session is already assigned to
     *            servers, this operation does nothing.
     */
    public void assignToServers(final WebSession... sessions);

    /**
     * Registers a new application server with this load balancer.
     * 
     * @param vm
     *            - the new app server.
     */
    public void registerAppServer(final HddVm vm);
    
    /**
     * Returns the list of all app servers managed by this load balancer.
     * @return the list of all app servers managed by this load balancer.
     */
    public List<HddVm> getAppServers();
    
    /**
     * Returns the DB server managed by this load balancer.
     * @return the DB server managed by this load balancer.
     */
    public HddVm getDbServer();

}
