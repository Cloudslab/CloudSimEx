package org.cloudbus.cloudsim.ex.web;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;

import org.cloudbus.cloudsim.ex.disk.HddResCloudlet;
import org.cloudbus.cloudsim.ex.disk.HddVm;
import org.cloudbus.cloudsim.ex.util.Id;

/**
 * Implements simple load balancing - sessions are assigned to the least busy
 * (in terms of CPU) application server VMs.
 * 
 * @author nikolay.grozev
 * 
 */
public class SimpleWebLoadBalancer implements ILoadBalancer {

    private final long id;
    private final List<HddVm> appServers;

    private final IDBBalancer dbBalancer;

    private long startPositionWhenEqual = 0;

    /**
     * Constructor.
     * 
     * @param appServers
     *            - the application servers. Must not be null.
     * @param dbBalancer
     *            - the balancer of the DB cloudlets among DB servers. Must not
     *            be null.
     */
    public SimpleWebLoadBalancer(final List<HddVm> appServers, final IDBBalancer dbBalancer) {
	super();
	id = Id.pollId(SimpleWebLoadBalancer.class);
	this.appServers = appServers;
	this.dbBalancer = dbBalancer;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.cloudbus.cloudsim.incubator.web.ILoadBalancer#getId()
     */
    @Override
    public long getId() {
	return id;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.cloudbus.cloudsim.incubator.web.ILoadBalancer#registerAppServer(org
     * .cloudbus.cloudsim.incubator.web.extensions.HddVm)
     */
    @Override
    public void registerAppServer(final HddVm vm) {
	appServers.add(vm);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.cloudbus.cloudsim.incubator.web.ILoadBalancer#assignToServers(org
     * .cloudbus.cloudsim.incubator.web.WebSession)
     */
    @Override
    public void assignToServers(final WebSession... sessions) {
	// Filter all sessions without an assigned application server
	List<WebSession> noAppServSessions = new ArrayList<>();
	noAppServSessions.addAll(Arrays.asList(sessions));
	for (ListIterator<WebSession> iter = noAppServSessions.listIterator(); iter.hasNext();) {
	    WebSession sess = iter.next();
	    if (sess.getAppVmId() != null) {
		iter.remove();
	    }
	}

	// Get the VMs which are utilized the least
	List<HddVm> bestVms = new ArrayList<>();
	double bestUtilization = Double.MAX_VALUE;
	for (HddVm vm : appServers) {
	    double vmUtilization = evaluateUtilization(vm);
	    if (!vm.isOutOfMemory()) {
		if (vmUtilization < bestUtilization) {
		    bestVms.clear();
		    bestUtilization = vmUtilization;
		    bestVms.add(vm);
		} else if (vmUtilization == bestUtilization) {
		    bestVms.add(vm);
		}
	    }
	}

	// Distribute the sessions among the best VMs
	long i = startPositionWhenEqual++;
	if (!bestVms.isEmpty()) {
	    for (WebSession session : noAppServSessions) {
		long index = i++ % bestVms.size();
		session.setAppVmId(bestVms.get((int) index).getId());
	    }
	}

	// Set the DB VM
	for (WebSession session : sessions) {
	    if (session.getDbBalancer() == null) {
		session.setDbBalancer(getDbBalancer());
	    }
	}
    }

    private static double evaluateUtilization(final HddVm vm) {
	double sumExecCloudLets = 0;
	for (HddResCloudlet cloudlet : vm.getCloudletScheduler().getCloudletExecList()) {
	    sumExecCloudLets += cloudlet.getCloudletLength();
	}
	double vmMips = vm.getMips() * vm.getNumberOfPes();
	return sumExecCloudLets / vmMips;
    }

    @Override
    public List<HddVm> getAppServers() {
	return appServers;
    }

    @Override
    public IDBBalancer getDbBalancer() {
	return dbBalancer;
    }
}
