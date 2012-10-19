package org.cloudbus.cloudsim.incubator.web.extensions;

import java.util.List;

import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmScheduler;
import org.cloudbus.cloudsim.incubator.util.Id;
import org.cloudbus.cloudsim.lists.PeList;
import org.cloudbus.cloudsim.provisioners.BwProvisioner;
import org.cloudbus.cloudsim.provisioners.RamProvisioner;

/**
 * A host with one or several harddisks.
 * 
 * @author nikolay.grozev
 * 
 */
public class HddHost extends Host {

    /** The list of harddisks. */
    private final List<? extends HDPe> hddList;
    /** A scheduler for the harddisk operations. */
    private final VmScheduler hddIOScheduler;

    /**
     * Constructor.
     * 
     * @param ramProvisioner
     * @param bwProvisioner
     * @param storage
     * @param peList
     * @param hddList
     * @param vmCPUScheduler
     *            - the CPU scheduler.
     * @param vmHDDScheduler
     *            - the IO scheduler.
     */
    public HddHost(final RamProvisioner ramProvisioner, final BwProvisioner bwProvisioner, final long storage,
	    final List<? extends Pe> peList, final List<? extends HDPe> hddList, final VmScheduler vmCPUScheduler,
	    final VmScheduler vmHDDScheduler) {
	super(Id.pollId(HddHost.class), ramProvisioner, bwProvisioner, storage, peList, vmCPUScheduler);
	this.hddIOScheduler = vmHDDScheduler;
	this.hddList = hddList;
	setFailed(false);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.cloudbus.cloudsim.Host#updateVmsProcessing(double)
     */
    @Override
    public double updateVmsProcessing(final double currentTime) {
	double smallerTime = Double.MAX_VALUE;

	for (HddVm vm : getVmList()) {
	    List<Double> mips = getVmScheduler().getAllocatedMipsForVm(vm);
	    List<Double> iops = getHddIOScheduler().getAllocatedMipsForVm(vm);
	    double time = vm.updateVmProcessing(currentTime, mips, iops);

	    if (time > 0.0 && time < smallerTime) {
		smallerTime = time;
	    }
	}

	return smallerTime;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.cloudbus.cloudsim.Host#vmCreate(org.cloudbus.cloudsim.Vm)
     */
    @Override
    public boolean vmCreate(Vm vm) {
	boolean allocatednOfCPUFlag = super.vmCreate(vm);
	boolean allocationOfHDD = false;

	allocationOfHDD = allocatednOfCPUFlag && getHddIOScheduler().allocatePesForVm(vm, ((HddVm)vm).getCurrentRequestedIOMips());

	if (allocatednOfCPUFlag && !allocationOfHDD) {
	    getRamProvisioner().deallocateRamForVm(vm);
	    getBwProvisioner().deallocateBwForVm(vm);
	    deallocatePesForVm(vm);
	}

	return allocationOfHDD;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.cloudbus.cloudsim.Host#getVmList()
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public List<HddVm> getVmList() {
	return (List) super.getVmList();
    }

    /**
     * Returns the numbder of unused/free harddrives.
     * 
     * @return the numbder of unused/free harddrives.
     */
    public int getNumberOfFreeHdds() {
	return PeList.getNumberOfFreePes(getHddList());
    }

    /**
     * Returns the total MIPS.
     * 
     * @return the total MIPS.
     */
    public int getTotalIOMips() {
	return PeList.getTotalMips(getHddList());
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.cloudbus.cloudsim.Host#setFailed(boolean)
     */
    @Override
    public boolean setFailed(boolean failed) {
	if (getHddList() != null) {
	    PeList.setStatusFailed(getHddList(), failed);
	}
	return super.setFailed(failed);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.cloudbus.cloudsim.Host#setFailed(java.lang.String, boolean)
     */
    @Override
    public boolean setFailed(final String resName, final boolean failed) {
	PeList.setStatusFailed(getHddList(), resName, getId(), failed);
	return super.setFailed(resName, failed);
    }

    /**
     * Sets the status of the harddisk with the specified id.
     * 
     * @param peId
     *            - the id of the harddisk.
     * @param status
     *            - the new status.
     * @return if the status was set correctly0.
     */
    public boolean setHddStatus(final int peId, final int status) {
	return PeList.setPeStatus(getHddList(), peId, status);
    }

    /**
     * Returns the list of all hard disks of this host.
     * 
     * @return the list of all hard disks of this host.
     */
    public List<? extends HDPe> getHddList() {
	return hddList;
    }

    /**
     * Returns the scheduler, that manages the distribution of the I/O
     * operations among VMs.
     * 
     * @return the scheduler, that manages the distribution of the I/O
     *         operations among VMs.
     */
    public VmScheduler getHddIOScheduler() {
	return hddIOScheduler;
    }

}
