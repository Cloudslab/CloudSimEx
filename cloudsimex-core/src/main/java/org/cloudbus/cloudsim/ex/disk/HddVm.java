package org.cloudbus.cloudsim.ex.disk;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;

import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.ex.VmSchedulerMapVmsToPes;
import org.cloudbus.cloudsim.ex.util.Id;

/**
 * A virtual machine with a harddisk. Unlike the other CloudSim implementations
 * of virtual machines, this one models the contention caused by the I/O
 * operations of the cloudlets.
 * 
 * @author nikolay.grozev
 * 
 */
public class HddVm extends Vm {

    /** The IO MIPS. */
    private double ioMips;
    private final LinkedHashSet<Integer> hdds = new LinkedHashSet<>();
    private boolean outOfMemory = false;

    /**
     * Constr.
     * 
     * @param userId
     *            - see parent class.
     * @param mips
     *            - see parent class.
     * @param numberOfPes
     *            - see parent class.
     * @param ram
     *            - see parent class.
     * @param bw
     *            - see parent class.
     * @param size
     *            - see parent class.
     * @param vmm
     *            - see parent class.
     * @param cloudletScheduler
     *            - the scheduler that will schedule the disk and CPU operations
     *            among cloudlets.
     * @param hddIds
     *            - a list of ids of the harddisks that this VM has access to.
     *            If empty the VM has access to all disks of the host, which i
     *            hosting it at a given time.
     */
    public HddVm(final int userId, final double mips, final double ioMips, final int numberOfPes, final int ram,
	    final long bw, final long size, final String vmm,
	    final HddCloudletSchedulerTimeShared cloudletScheduler, final Integer... hddIds) {
	super(Id.pollId(HddVm.class), userId, mips, numberOfPes, ram, bw, size, vmm, cloudletScheduler);
	this.ioMips = ioMips;
	this.hdds.addAll(Arrays.asList(hddIds));
	cloudletScheduler.setVm(this);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.cloudbus.cloudsim.Vm#updateVmProcessing(double, java.util.List)
     */
    @Override
    @Deprecated
    public double updateVmProcessing(final double currentTime, final List<Double> mipsShare) {
	return updateVmProcessing(currentTime, mipsShare, Arrays.<Double> asList());
    }

    /**
     * Updates the processing of the VM.
     * 
     * @param currentTime
     *            - current simulation time.
     * @param mipsShare
     *            - array with MIPS share of each Pe available to the scheduler.
     * @param iopsShare
     *            - array with I/O MIPS share of each Harddisk available to the
     *            scheduler.
     * @return
     */
    public double updateVmProcessing(final double currentTime, final List<Double> mipsShare,
	    final List<Double> iopsShare) {
	if (mipsShare != null && iopsShare != null) {
	    return getCloudletScheduler().updateVmProcessing(currentTime, mipsShare, iopsShare);
	}
	return 0.0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.cloudbus.cloudsim.Vm#getCloudletScheduler()
     */
    @Override
    public HddCloudletSchedulerTimeShared getCloudletScheduler() {
	return (HddCloudletSchedulerTimeShared) super.getCloudletScheduler();
    }

    public double getIoMips() {
	return ioMips;
    }

    public void setIoMips(final double ioMips) {
	this.ioMips = ioMips;
    }

    /**
     * Returns the number of the harddisks, that this VM can use.
     * 
     * @return the number of the harddisks, that this VM can use.
     */
    public int getNumberOfHdds() {
	return hdds.isEmpty() && getHost() != null ? getHost().getNumberOfHdds() : hdds.size();
    }

    /**
     * Returns the ids of the harddisks, that this VM can use.
     * 
     * @return the ids of the harddisks, that this VM can use.
     */
    public LinkedHashSet<Integer> getHddsIds() {
	LinkedHashSet<Integer> result = hdds;
	if (hdds.isEmpty() && getHost() != null) {
	    result = new LinkedHashSet<>();
	    for (HddPe hdd : getHost().getHddList()) {
		result.add(hdd.getId());
	    }
	}
	return result;
    }

    @Override
    public List<Double> getCurrentRequestedMips() {
	if (getHost().getVmScheduler() instanceof VmSchedulerMapVmsToPes) {
	    VmSchedulerMapVmsToPes<?> scheduler = (VmSchedulerMapVmsToPes<?>) getHost().getVmScheduler();

	    List<Double> currentRequestedMips = getCloudletScheduler().getCurrentRequestedMips();
	    if (isBeingInstantiated()) {
		currentRequestedMips = new ArrayList<Double>();
		for (Pe pe : getHost().getPeList()) {
		    if (scheduler.doesVmUse(this, pe)) {
			currentRequestedMips.add(getMips());
		    } else {
			currentRequestedMips.add(0.0);
		    }
		}
		for (int i = 0; i < getNumberOfPes(); i++) {
		    currentRequestedMips.add(getMips());
		}
	    }
	    return currentRequestedMips;

	} else {
	    return super.getCurrentRequestedMips();
	}
    }

    /**
     * Returns a mapping between hdd ids and required miops.
     * 
     * @return - a mapping between hdd ids and required miops.
     */

    public List<Double> getCurrentRequestedIOMips() {
	List<Double> currentRequestedMips = getCloudletScheduler().getCurrentRequestedIOMips();
	if (isBeingInstantiated()) {
	    currentRequestedMips = new ArrayList<Double>();

	    // Put zeros for the harddisks we don't have access to from this VM
	    for (HddPe hdd : getHost().getHddList()) {
		if (this.getHddsIds().contains(hdd.getId())) {
		    currentRequestedMips.add(getIoMips());
		} else {
		    currentRequestedMips.add(0.0);
		}

	    }
	}
	return currentRequestedMips;
    }

    public boolean isOutOfMemory() {
	return outOfMemory;
    }

    public void setOutOfMemory(final boolean outOfMemory) {
	this.outOfMemory = outOfMemory;
    }

    @Override
    public HddHost getHost() {
	return (HddHost) super.getHost();
    }

}
