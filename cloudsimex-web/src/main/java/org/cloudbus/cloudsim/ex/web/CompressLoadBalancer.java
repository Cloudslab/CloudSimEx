package org.cloudbus.cloudsim.ex.web;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.logging.Level;

import org.cloudbus.cloudsim.ex.disk.HddVm;
import org.cloudbus.cloudsim.ex.util.CustomLog;
import org.cloudbus.cloudsim.ex.vm.MonitoredVMex;

/**
 * 
 * A load balancer, which compresses the load into the smallest number of VMs
 * whose utilisation is below certain thresholds.
 * 
 * @author nikolay.grozev
 * 
 */
public class CompressLoadBalancer extends BaseWebLoadBalancer implements ILoadBalancer {

    private static Comparator<MonitoredVMex> CPU_UTIL_INVERSE_CMP = new CPUUtilisationComparator();
    private final double cpuThreshold;
    private final double ramThreshold;

    /**
     * Const.
     * 
     * @param appId
     * @param ip
     * @param appServers
     * @param dbBalancer
     * @param cpuThreshold
     *            - the CPU threshold. Must be in the interval [0, 1].
     * @param ramThreshold
     *            - the RAM threshold. Must be in the interval [0, 1].
     */
    public CompressLoadBalancer(final long appId, final String ip, final List<HddVm> appServers,
	    final IDBBalancer dbBalancer, double cpuThreshold, double ramThreshold) {
	super(appId, ip, appServers, dbBalancer);
	this.cpuThreshold = cpuThreshold;
	this.ramThreshold = ramThreshold;
    }

    @Override
    public void assignToServers(final WebSession... sessions) {
	// Filter all sessions without an assigned application server
	List<WebSession> noAppServSessions = new ArrayList<>(Arrays.asList(sessions));
	for (ListIterator<WebSession> iter = noAppServSessions.listIterator(); iter.hasNext();) {
	    WebSession sess = iter.next();
	    if (sess.getAppVmId() != null) {
		iter.remove();
	    }
	}

	List<HddVm> runingVMs = getRunningAppServers();
	// No running AS servers - log an error
	if (runingVMs.isEmpty()) {
	    for (WebSession session : noAppServSessions) {
		if (getAppServers().isEmpty()) {
		    CustomLog.printf(Level.SEVERE,
			    "Session %d cannot be scheduled, as there are not AS servers",
			    session.getSessionId());
		} else {
		    CustomLog.printf(Level.SEVERE,
			    "Session %d cannot be scheduled, as all AS servers are either booting or terminated",
			    session.getSessionId());
		}
	    }
	} else {// Assign to one of the running VMs
	    for (WebSession session : noAppServSessions) {
		List<HddVm> vms = new ArrayList<>(runingVMs);
		Collections.sort(vms, CPU_UTIL_INVERSE_CMP);

		HddVm bestVm = vms.get(vms.size() - 1);
		for (HddVm vm : vms) {
		    if (vm.getCPUUtil() < cpuThreshold && vm.getRAMUtil() < ramThreshold && !vm.isOutOfMemory()) {
			bestVm = vm;
		    }
		}

		session.setAppVmId(bestVm.getId());
	    }

	    // Set the DB VM
	    for (WebSession session : sessions) {
		if (session.getDbBalancer() == null) {
		    session.setDbBalancer(getDbBalancer());
		}
	    }
	}
    }

    private static class CPUUtilisationComparator implements Comparator<MonitoredVMex> {

	@Override
	public int compare(final MonitoredVMex vm1, final MonitoredVMex vm2) {
	    return -Double.valueOf(vm1.getCPUUtil()).compareTo(Double.valueOf(vm2.getCPUUtil()));
	}
    }

}
