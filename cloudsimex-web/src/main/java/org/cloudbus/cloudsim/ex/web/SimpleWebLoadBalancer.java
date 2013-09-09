package org.cloudbus.cloudsim.ex.web;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;

import org.cloudbus.cloudsim.ex.disk.HddResCloudlet;
import org.cloudbus.cloudsim.ex.disk.HddVm;

/**
 * Implements simple load balancing - sessions are assigned to the least busy
 * (in terms of CPU) application server VMs.
 * 
 * @author nikolay.grozev
 * 
 */
public class SimpleWebLoadBalancer extends BaseWebLoadBalancer implements ILoadBalancer {

    private long startPositionWhenEqual = 0;

    /**
     * Constructor.
     * 
     * @param appId
     *            - the id of the applications, which this load balancer is
     *            serving.
     * @param ip - the IP address represented in a standard Ipv4 or IPv6 dot notation.
     * @param appServers
     *            - the application servers. Must not be null.
     * @param dbBalancer
     *            - the balancer of the DB cloudlets among DB servers. Must not
     *            be null.
     */
    public SimpleWebLoadBalancer(final long appId, final String ip, final List<HddVm> appServers, final IDBBalancer dbBalancer) {
	super(appId, ip, appServers, dbBalancer);
    }

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
	for (HddVm vm : getRunningAppServers()) {
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
	for (HddResCloudlet cloudlet : vm.getCloudletScheduler().<HddResCloudlet>getCloudletExecList()) {
	    sumExecCloudLets += cloudlet.getCloudletLength();
	}
	double vmMips = vm.getMips() * vm.getNumberOfPes();
	return sumExecCloudLets / vmMips;
    }
}
