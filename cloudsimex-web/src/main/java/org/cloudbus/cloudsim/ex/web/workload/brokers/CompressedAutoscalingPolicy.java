package org.cloudbus.cloudsim.ex.web.workload.brokers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.ex.IAutoscalingPolicy;
import org.cloudbus.cloudsim.ex.MonitoringBorkerEX;
import org.cloudbus.cloudsim.ex.billing.IVmBillingPolicy;
import org.cloudbus.cloudsim.ex.disk.HddCloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.ex.disk.HddVm;
import org.cloudbus.cloudsim.ex.web.ILoadBalancer;
import org.cloudbus.cloudsim.ex.web.WebSession;

/**
 * 
 * An autoscaling policy, which proactively allocates and releases resources for
 * a 3-tier application.
 * 
 * @author nikolay.grozev
 * 
 */
public class CompressedAutoscalingPolicy implements IAutoscalingPolicy {

    private long appId;
    private double triggerCPU;
    private double triggerRAM;
    private int n;

    
    public CompressedAutoscalingPolicy(long appId, double triggerCPU, double triggerRAM, int n) {
	super();
	this.appId = appId;
	this.triggerCPU = triggerCPU;
	this.triggerRAM = triggerRAM;
	this.n = n;
    }

    @Override
    public void scale(final MonitoringBorkerEX broker) {
	if (broker instanceof WebBroker) {
	    WebBroker webBroker = (WebBroker) broker;
	    ILoadBalancer loadBalancer = webBroker.getLoadBalancers().get(appId);
	    Map<Integer, Integer> asToNumSess = constructASToNumberOfSessionsTable(webBroker);

	    boolean allOverloaded = true;
	    List<HddVm> freeVms = new ArrayList<>();

	    // Inspect the status of all AS VMs
	    for (HddVm vm : loadBalancer.getAppServers()) {
		double vmCPU = vm.getCPUUtil();
		double vmRAM = vm.getRAMUtil();
		if (!asToNumSess.containsKey(vm.getId())) {
		    freeVms.add(vm);
		} else if (vmCPU < triggerCPU || vmRAM < triggerRAM) {
		    allOverloaded = false;
		}
	    }

	    int numFree = freeVms.size();

	    if (numFree <= n) { // Provision more VMs..
		int numVmsToStart = 0;
		if (allOverloaded) {
		    numVmsToStart = n - numFree + 1;
		} else {
		    numVmsToStart = n - numFree;
		}
		// Start numVmsToStart new AS VMs
		startASVms(webBroker, loadBalancer, numVmsToStart);
	    } else { // Release VMs
		int numVmsToStop = 0;
		if (allOverloaded) {
		    numVmsToStop = numFree - n;
		} else {
		    numVmsToStop = numFree - n + 1;
		}

		List<HddVm> toStop = new ArrayList<>();
		Collections.sort(freeVms, new CloudPriceComparator(webBroker.getVMBillingPolicy()));
		for (int i = 0; i < numVmsToStop; i++) {
		    double billTime = webBroker.getVMBillingPolicy().nexChargeTime(freeVms.get(i));
		    int delta = 10;
		    if (billTime - CloudSim.clock() < delta) {
			toStop.add(freeVms.get(i));
		    } else {
			break;
		    }
		}
		webBroker.destroyVMsAfter(toStop, 0);
	    }
	}
    }

    private void startASVms(WebBroker webBroker, ILoadBalancer loadBalancer, int numVmsToStart) {
	List<HddVm> newVMs = new ArrayList<>();
	for(int i = 0; i < numVmsToStart; i++) {
	    HddVm newASServer = loadBalancer.getAppServers().get(0).clone(new HddCloudletSchedulerTimeShared());
	    loadBalancer.registerAppServer(newASServer);
	}
	webBroker.createVmsAfter(newVMs, 0);
    }

    private Map<Integer, Integer> constructASToNumberOfSessionsTable(final WebBroker broker) {
	Map<Integer, Integer> result = new HashMap<>();
	for (WebSession session : broker.getServedSessions()) {
	    if (!session.isComplete()) {
		result.put(session.getAppVmId(),
			result.containsKey(session.getAppVmId()) ? result.get(session.getAppVmId()) + 1 : 1);
	    }
	}
	return result;
    }

    private static class CloudPriceComparator implements Comparator<HddVm> {
	private IVmBillingPolicy policy;

	public CloudPriceComparator(final IVmBillingPolicy policy) {
	    super();
	    this.policy = policy;
	}

	@Override
	public int compare(final HddVm vm1, final HddVm vm2) {
	    return Double.valueOf(policy.nexChargeTime(vm1)).compareTo(policy.nexChargeTime(vm2));
	}

    }

}
