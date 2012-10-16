package org.cloudbus.cloudsim.incubator.web.extensions;

import java.util.List;

import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.VmScheduler;
import org.cloudbus.cloudsim.incubator.util.Id;
import org.cloudbus.cloudsim.lists.PeList;
import org.cloudbus.cloudsim.provisioners.BwProvisioner;
import org.cloudbus.cloudsim.provisioners.RamProvisioner;

public class HddHost extends Host {

    /** The list of harddisks. */
    private final List<? extends HDPe> hddList;
    private final VmScheduler hddIOScheduler;

    public HddHost(final RamProvisioner ramProvisioner, final BwProvisioner bwProvisioner, final long storage,
	    final List<? extends Pe> peList, final List<? extends HDPe> hddList, final VmScheduler vmCPUScheduler,
	    final VmScheduler vmHDDScheduler) {
	super(Id.pollId(HddHost.class), ramProvisioner, bwProvisioner, storage, peList, vmCPUScheduler);
	this.hddIOScheduler = vmHDDScheduler;
	this.hddList = hddList;
    }

    /**
     * (non-Javadoc)
     * @see org.cloudbus.cloudsim.Host#updateVmsProcessing(double)
     */
    @Override
    public double updateVmsProcessing(final double currentTime) {
	double smallerTime = Double.MAX_VALUE;

	for (HDVm vm : getVmList()) {
	    double time = vm.updateVmProcessing(currentTime, getVmScheduler().getAllocatedMipsForVm(vm),
		    getHddIOScheduler().getAllocatedMipsForVm(vm));

	    if (time > 0.0 && time < smallerTime) {
		smallerTime = time;
	    }
	}

	return smallerTime;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public List<HDVm> getVmList() {
	return (List) super.getVmList();
    }

    public int getNumberOfFreeHdds() {
	return PeList.getNumberOfFreePes(getHddList());
    }

    public int getTotalIOMips() {
	return PeList.getNumberOfFreePes(getHddList());
    }

    @Override
    public boolean setFailed(boolean failed) {
	PeList.setStatusFailed(getHddList(), failed);
	return super.setFailed(failed);
    }

    @Override
    public boolean setFailed(final String resName, final boolean failed) {
	PeList.setStatusFailed(getHddList(), resName, getId(), failed);
	return super.setFailed(resName, failed);
    }

    public boolean setHddStatus(final int peId, final int status) {
	return PeList.setPeStatus(getHddList(), peId, status);
    }

    public List<? extends HDPe> getHddList() {
	return hddList;
    }

    public VmScheduler getHddIOScheduler() {
	return hddIOScheduler;
    }

}
