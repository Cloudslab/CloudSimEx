package org.cloudbus.cloudsim.ex.web.experiments;

import java.util.List;

import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.ex.disk.HddResCloudlet;
import org.cloudbus.cloudsim.ex.disk.HddVm;
import org.cloudbus.cloudsim.ex.util.CustomLog;
import org.cloudbus.cloudsim.ex.web.ILoadBalancer;
import org.cloudbus.cloudsim.ex.web.WebBroker;

public class LoggingWebBroker extends WebBroker {

    private boolean headerPrinted = false;

    public LoggingWebBroker(final String name, final double refreshPeriod, final double lifeLength) throws Exception {
	super(name, refreshPeriod, lifeLength);
    }

    public LoggingWebBroker(final String name, final double refreshPeriod,
	    final double lifeLength, final List<Integer> dataCenterIds) throws Exception {
	super(name, refreshPeriod, lifeLength, dataCenterIds);
    }

    @Override
    protected void processOtherEvent(final SimEvent ev) {
	switch (ev.getTag()) {
	    case TIMER_TAG:
		logUtilisation();
		break;
	}
	super.processOtherEvent(ev);
    }

    private void logUtilisation() {
	if (!headerPrinted) {
	    CustomLog.printHeader(UtilisationEntry.class);
	    headerPrinted = true;
	}
	for (ILoadBalancer balancer : getLoadBalancers().values()) {
	    for (HddVm vm : balancer.getAppServers()) {
		logUtilisation(vm);
	    }
	    for (HddVm vm : balancer.getDbBalancer().getVMs()) {
		logUtilisation(vm);
	    }
	}
    }

    private void logUtilisation(final HddVm vm) {
	UtilisationEntry utilEntry = new UtilisationEntry(CloudSim.clock(),
		vm.getId(),
		100 * evaluateCPUUtilization(vm),
		100 * evaluateIOUtilization(vm),
		100 * evaluateRAMUtilization(vm));
	CustomLog.printLineForObject(utilEntry);
    }

    private static double evaluateCPUUtilization(final HddVm vm) {
	double sumExecCloudLets = 0;
	for (HddResCloudlet cloudlet : vm.getCloudletScheduler().getCloudletExecList()) {
	    sumExecCloudLets += cloudlet.getCloudletLength();
	}
	double vmMips = vm.getMips() * vm.getNumberOfPes();
	return sumExecCloudLets / vmMips;
    }

    private static double evaluateIOUtilization(final HddVm vm) {
	double sumExecCloudLets = 0;
	for (HddResCloudlet cloudlet : vm.getCloudletScheduler().getCloudletExecList()) {
	    sumExecCloudLets += cloudlet.getCloudlet().getCloudletIOLength();
	}
	double vmIOMips = vm.getIoMips();
	return sumExecCloudLets / vmIOMips;
    }

    private static double evaluateRAMUtilization(final HddVm vm) {
	double sumExecCloudLets = 0;
	for (HddResCloudlet cloudlet : vm.getCloudletScheduler().getCloudletExecList()) {
	    sumExecCloudLets += cloudlet.getCloudlet().getRam();
	}
	double vmRam = vm.getRam();
	return sumExecCloudLets / vmRam;
    }

    private static class UtilisationEntry {
	private final double time;
	private final int vmId;
	private final double percentCPU;
	private final double percentIO;
	private final double percentRAM;

	public UtilisationEntry(final double time,
		final int vmId,
		final double percentCPU,
		final double percentIO,
		final double percentRAM) {
	    super();
	    this.time = time;
	    this.vmId = vmId;
	    this.percentCPU = percentCPU;
	    this.percentIO = percentIO;
	    this.percentRAM = percentRAM;
	}

	public double getTime() {
	    return time;
	}

	public int getVmId() {
	    return vmId;
	}

	public double getPercentCPU() {
	    return percentCPU;
	}

	public double getPercentIO() {
	    return percentIO;
	}

	public double getPercentRAM() {
	    return percentRAM;
	}
    }

}
