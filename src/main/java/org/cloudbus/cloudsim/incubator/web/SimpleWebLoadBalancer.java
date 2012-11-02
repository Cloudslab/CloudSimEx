package org.cloudbus.cloudsim.incubator.web;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;

import org.cloudbus.cloudsim.incubator.util.Id;
import org.cloudbus.cloudsim.incubator.web.extensions.HddResCloudlet;
import org.cloudbus.cloudsim.incubator.web.extensions.HddVm;

/**
 * Implements simple load balancing - sessions are assigned to the least busy
 * (in terms of CPU) application server VMs.
 * 
 * @author nikolay.grozev
 * 
 */
public class SimpleWebLoadBalancer implements ILoadBalancer {

    private List<HddVm> appServers;
    private HddVm dbServer;
    private long id;

    /**
     * Constructor.
     * 
     * @param appServers
     *            - the application servers. Must not be null.
     * @param dbServer
     *            - the database server. Must not be null.
     */
    public SimpleWebLoadBalancer(List<HddVm> appServers, HddVm dbServer) {
	super();
	id = Id.pollId(SimpleWebLoadBalancer.class);
	this.appServers = appServers;
	this.dbServer = dbServer;
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
    public void registerAppServer(HddVm vm) {
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
	List<WebSession> appServSessions = new ArrayList<>();
	appServSessions.addAll(Arrays.asList(sessions));
	for (ListIterator<WebSession> iter = appServSessions.listIterator(); iter.hasNext();) {
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
	int i = 0;
	if (!bestVms.isEmpty()) {
	    for (WebSession session : appServSessions) {
		int index = i++ % bestVms.size();
		session.setAppVmId(bestVms.get(index).getId());
	    }
	}

	// Set the DB VM
	for (WebSession session : sessions) {
	    if (session.getDbVmId() == null && !dbServer.isOutOfMemory()) {
		session.setDbVmId(dbServer.getId());
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
    public HddVm getDbServer() {
	return dbServer;
    }
}
