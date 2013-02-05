/*
 * Title: CloudSim Toolkit Description: CloudSim (Cloud Simulation) Toolkit for Modeling and
 * Simulation of Clouds Licence: GPL - http://www.gnu.org/copyleft/gpl.html
 * 
 * Copyright (c) 2009-2012, The University of Melbourne, Australia
 */

package org.cloudbus.cloudsim.incubator.disk;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.logging.Level;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletScheduler;
import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.ResCloudlet;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.incubator.disk.HddResCloudlet;
import org.cloudbus.cloudsim.incubator.util.CustomLog;
import org.cloudbus.cloudsim.incubator.web.WebCloudlet;
import org.cloudbus.cloudsim.lists.ResCloudletList;

/**
 * HddCloudletSchedulerTimeShared implements a policy of scheduling performed by
 * a virtual machine. Unlike other cloudlet schedulers, this one takes into
 * account the I/O operations required by the cloudlet.
 * 
 * <br>
 * <br>
 * 
 * Unlike other {@link CloudletScheduler}s this class keeps a reference to the
 * VM, whose cloudlets are scheduled. Thus one needs to set the vm before using
 * an instance of this scheduler.
 * 
 * <br>
 * Adapted from code of:
 * <ul>
 * <li>Rodrigo N. Calheiros</li>
 * <li>Anton Beloglazov</li>
 * </ul>
 * 
 * @author nikolay.grozev
 * 
 * 
 */
public class HddCloudletSchedulerTimeShared extends CloudletScheduler {

    /** The cloudlet exec list. */
    private List<? extends HddResCloudlet> cloudletExecList;

    /** The cloudlet paused list. */
    private List<? extends HddResCloudlet> cloudletPausedList;

    /** The cloudlet finished list. */
    private List<? extends HddResCloudlet> cloudletFinishedList;

    /** The cloudlet failed list. */
    private List<? extends HddResCloudlet> cloudletFailedList;

    /** The current IO mips share. */
    private List<Double> currentIOMipsShare;

    /** The VM being scheduled. */
    private HddVm vm;

    /**
     * Creates a new CloudletSchedulerTimeShared object. This method must be
     * invoked before starting the actual simulation.
     * 
     * @pre $none
     * @post $none
     */
    public HddCloudletSchedulerTimeShared() {
	super();
	cloudletExecList = new ArrayList<>();
	cloudletPausedList = new ArrayList<>();
	cloudletFinishedList = new ArrayList<>();
	cloudletFailedList = new ArrayList<>();
    }

    public HddVm getVm() {
	return vm;
    }

    public void setVm(final HddVm vm) {
	this.vm = vm;
    }

    /**
     * Returns the current IO Mips share.
     * 
     * @return the current IO Mips share.
     */
    public List<Double> getCurrentIOMipsShare() {
	return currentIOMipsShare;
    }

    /**
     * Sets the current IO Mips share.
     * 
     * @param currentIOMipsShare
     *            - the current IO Mips share.
     */
    public void setCurrentIOMipsShare(final List<Double> currentIOMipsShare) {
	this.currentIOMipsShare = currentIOMipsShare;
    }

    /**
     * Updates the processing of cloudlets running under management of this
     * scheduler.
     * 
     * @param currentTime
     *            current simulation time
     * @param mipsShare
     *            array with MIPS share of each processor available to the
     *            scheduler
     * @return time predicted completion time of the earliest finishing
     *         cloudlet, or 0 if there is no next events
     * @pre currentTime >= 0
     * @post $none
     */
    @Override
    public double updateVmProcessing(final double currentTime, final List<Double> mipsShare) {
	return updateVmProcessing(currentTime, mipsShare, Arrays.<Double> asList());
    }

    /**
     * 
     * @param currentTime
     * @param mipsShare
     * @param iopsShare
     * @return
     */
    public double updateVmProcessing(final double currentTime, final List<Double> mipsShare,
	    final List<Double> iopsShare) {

	CustomLog.printf(Level.FINEST, "\nupdateVmProcessing(currentTime=%f, mipsShare=%s,final iopsShare=%s)",
		currentTime, mipsShare.toString(), iopsShare.toString());

	setCurrentMipsShare(mipsShare);
	setCurrentIOMipsShare(iopsShare);
	double timeSpam = currentTime - getPreviousTime();

	List<Long[]> finishedSoFar = new ArrayList<>();
	for (HddResCloudlet rcl : getCloudletExecList()) {
	    long cpuFinishedSoFar =
		    (long) (getCPUCapacity(mipsShare) * timeSpam * rcl.getNumberOfPes() * Consts.MILLION);
	    long ioFinishedSoFar =
		    (long) (getIOCapacity(iopsShare, rcl) * timeSpam * rcl.getNumberOfHdds() * Consts.MILLION);
	    finishedSoFar.add(new Long[] { cpuFinishedSoFar, ioFinishedSoFar });
	}

	int i = 0;
	for (HddResCloudlet rcl : getCloudletExecList()) {
	    Long[] updates = finishedSoFar.get(i++);
	    rcl.updateCloudletFinishedSoFar(updates[0], updates[1]);

	    CustomLog.printf(Level.FINEST,
		    "cloudlet=%d, cpuFinishedSoFar=%d, ioFinishedSoFar=%d, CPUleft=%d, leftIO=%d ",
		    rcl.getCloudlet().getCloudletId(),
		    updates[0] / Consts.MILLION,
		    updates[1] / Consts.MILLION,
		    rcl.getRemainingCloudletLength(),
		    rcl.getRemainingCloudletIOLength());
	}

	if (getCloudletExecList().size() == 0) {
	    setPreviousTime(currentTime);
	    return 0.0;
	}

	removeFinishedCloudlets();
	double nextEvent = computeNextEventTime(currentTime, mipsShare, iopsShare);

	setPreviousTime(currentTime);
	return nextEvent;
    }

    private double computeNextEventTime(final double currentTime, final List<Double> mipsShare,
	    final List<Double> iopsShare) {
	// check finished cloudlets
	double nextEvent = Double.MAX_VALUE;
	// estimate finish time of cloudlets
	for (HddResCloudlet rcl : getCloudletExecList()) {
	    Double estimatedFinishCPUTime = rcl.getRemainingCloudletLength() == 0 ? null
		    :
		    currentTime
			    + (rcl.getRemainingCloudletLength() / (getCPUCapacity(mipsShare) * rcl.getNumberOfPes()));
	    Double estimatedFinishIOTime = rcl.getRemainingCloudletIOLength() == 0 ? null :
		    currentTime
			    + (rcl.getRemainingCloudletIOLength() / (getIOCapacity(iopsShare, rcl) * rcl
				    .getNumberOfHdds()));

	    Double estimatedFinishTime = refMin(estimatedFinishCPUTime, estimatedFinishIOTime);

	    if (estimatedFinishTime - currentTime < 0.1) {
		estimatedFinishTime = currentTime + 0.1;
	    }

	    if (estimatedFinishTime < nextEvent) {
		nextEvent = estimatedFinishTime;
	    }
	}
	return nextEvent;
    }

    private Double refMin(final Double estimatedFinishCPUTime, final Double estimatedFinishIOTime) {
	Double estimatedFinishTime = null;
	if (estimatedFinishCPUTime == null) {
	    estimatedFinishTime = estimatedFinishIOTime;
	} else if (estimatedFinishIOTime == null) {
	    estimatedFinishTime = estimatedFinishCPUTime;
	} else {
	    estimatedFinishTime = Math.min(estimatedFinishCPUTime, estimatedFinishIOTime);
	}
	return estimatedFinishTime;
    }

    private void removeFinishedCloudlets() {
	List<HddResCloudlet> toRemove = new ArrayList<HddResCloudlet>();
	for (HddResCloudlet rcl : getCloudletExecList()) {
	    long remainingLength = rcl.getRemainingCloudletLength();
	    long remainingIOLength = rcl.getRemainingCloudletIOLength();

	    if (remainingLength == 0 && remainingIOLength == 0) {
		toRemove.add(rcl);
		cloudletFinish(rcl);
		continue;
	    }
	}
	getCloudletExecList().removeAll(toRemove);
    }

    private double getIOCapacity(final List<Double> mipsShare, final HddResCloudlet rcl) {
	DataItem dataItem = rcl.getCloudlet().getData();
	double result = 0;
	if (dataItem != null) {
	    List<? extends HddPe> pes = getVm().getHost().getHddList();
	    int hddIndxInHost = -1;
	    for (int i = 0; i < pes.size(); i++) {
		if (pes.get(i).containsDataItem(dataItem.getId())) {
		    hddIndxInHost = i;
		    break;
		}
	    }

	    if (hddIndxInHost >= 0) {
		HddPe hdd = pes.get(hddIndxInHost);
		List<HddResCloudlet> execCloudlets = new ArrayList<>(getCloudletExecList());

		// Get the list of cloudlets that use this disk
		for (ListIterator<HddResCloudlet> iter = execCloudlets.listIterator(); iter.hasNext();) {
		    HddResCloudlet resCloudlet = iter.next();
		    DataItem cloudLetItem = resCloudlet.getCloudlet().getData();
		    // Does the cloudlet use the disk
		    if (cloudLetItem == null || !hdd.containsDataItem(cloudLetItem.getId())
			    || resCloudlet.getRemainingCloudletIOLength() <= 0) {
			iter.remove();
		    }
		}

		// The result is the IOPS of the harddisk divided by the number
		// of cloudlets using it
		result = mipsShare.get(hddIndxInHost) / execCloudlets.size();
	    }
	}
	return result;
    }

    private double getCPUCapacity(final List<Double> mipsShare) {
	double capacity = 0.0;
	int cpus = 0;
	for (Double mips : mipsShare) {
	    capacity += mips;
	    if (mips > 0.0) {
		cpus++;
	    }
	}

	int pesInUse = 0;
	for (HddResCloudlet rcl : getCloudletExecList()) {
	    if (rcl.getRemainingCloudletLength() != 0) {
		pesInUse += rcl.getNumberOfPes();
	    }
	}

	if (pesInUse > cpus) {
	    capacity /= pesInUse;
	} else {
	    capacity /= cpus;
	}
	return capacity;
    }

    /**
     * Cancels execution of a cloudlet.
     * 
     * @param cloudletId
     *            ID of the cloudlet being cancealed
     * @return the canceled cloudlet, $null if not found
     * @pre $none
     * @post $none
     */
    @Override
    public WebCloudlet cloudletCancel(final int cloudletId) {
	// First, looks in the finished queue
	int position = ResCloudletList.getPositionById(getCloudletFinishedList(), cloudletId);

	if (position >= 0) {
	    return getCloudletFinishedList().remove(position).getCloudlet();
	}

	// Then searches in the exec list
	position = ResCloudletList.getPositionById(getCloudletExecList(), cloudletId);

	if (position >= 0) {
	    HddResCloudlet rcl = getCloudletExecList().remove(position);
	    if (rcl.isDone()) {
		cloudletFinish(rcl);
	    } else {
		rcl.setCloudletStatus(WebCloudlet.CANCELED);
	    }
	    return rcl.getCloudlet();
	}

	// Now, looks in the paused queue
	position = ResCloudletList.getPositionById(getCloudletPausedList(), cloudletId);
	if (position >= 0) {
	    return getCloudletPausedList().remove(position).getCloudlet();
	}

	return null;
    }

    /**
     * Pauses execution of a cloudlet.
     * 
     * @param cloudletId
     *            ID of the cloudlet being paused
     * @return $true if cloudlet paused, $false otherwise
     * @pre $none
     * @post $none
     */
    @Override
    public boolean cloudletPause(final int cloudletId) {
	int position = ResCloudletList.getPositionById(getCloudletExecList(), cloudletId);

	if (position >= 0) {
	    // remove cloudlet from the exec list and put it in the paused list
	    HddResCloudlet rcl = getCloudletExecList().remove(position);
	    if (rcl.isDone()) {
		cloudletFinish(rcl);
	    } else {
		rcl.setCloudletStatus(WebCloudlet.PAUSED);
		getCloudletPausedList().add(rcl);
	    }
	    return true;
	}
	return false;
    }

    /**
     * Processes a finished cloudlet.
     * 
     * @param rcl
     *            finished cloudlet
     * @pre rgl != $null
     * @post $none
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void cloudletFinish(final ResCloudlet rcl) {
	rcl.setCloudletStatus(WebCloudlet.SUCCESS);
	rcl.finalizeCloudlet();
	((List) getCloudletFinishedList()).add(rcl);
    }

    /**
     * Resumes execution of a paused cloudlet.
     * 
     * @param cloudletId
     *            ID of the cloudlet being resumed
     * @return expected finish time of the cloudlet, 0.0 if queued
     * @pre $none
     * @post $none
     */
    // Changed
    @Override
    public double cloudletResume(final int cloudletId) {
	int position = ResCloudletList.getPositionById(getCloudletPausedList(), cloudletId);

	if (position >= 0) {
	    HddResCloudlet rgl = getCloudletPausedList().remove(position);
	    rgl.setCloudletStatus(WebCloudlet.INEXEC);
	    getCloudletExecList().add(rgl);

	    // calculate the expected time for cloudlet completion
	    // first: how many PEs do we have?

	    double remainingLength = rgl.getRemainingCloudletLength();
	    double remainingIOLength = rgl.getRemainingCloudletIOLength();
	    Double estimatedFinishCPUTime = remainingLength == 0 ? null :
		    CloudSim.clock()
			    + (remainingLength / (getCPUCapacity(getCurrentMipsShare()) * rgl.getNumberOfPes()));
	    Double estimatedFinishIOTime = remainingIOLength == 0 ? null
		    :
		    CloudSim.clock()
			    + (remainingIOLength / (getIOCapacity(getCurrentIOMipsShare(), rgl) * rgl.getNumberOfHdds()));

	    return refMin(estimatedFinishCPUTime, estimatedFinishIOTime);
	}

	return 0.0;
    }

    /**
     * Receives an cloudlet to be executed in the VM managed by this scheduler.
     * 
     * @param cloudlet
     *            the submited cloudlet
     * @param fileTransferTime
     *            time required to move the required files from the SAN to the
     *            VM
     * @return expected finish time of this cloudlet
     * @pre gl != null
     * @post $none
     */
    // Changed
    @Override
    public double cloudletSubmit(final Cloudlet cloudlet, final double fileTransferTime) {
	WebCloudlet hddCloudlet = (WebCloudlet) cloudlet;

	HddResCloudlet rcl = new HddResCloudlet(hddCloudlet);
	rcl.setCloudletStatus(WebCloudlet.INEXEC);
	for (int i = 0; i < hddCloudlet.getNumberOfPes(); i++) {
	    rcl.setMachineAndPeId(0, i);
	}

	getCloudletExecList().add(rcl);

	// use the current capacity to estimate the extra amount of
	// time to file transferring. It must be added to the cloudlet length
	double extraSize = getCPUCapacity(getCurrentMipsShare()) * fileTransferTime;
	long cpuLength = (long) (hddCloudlet.getCloudletLength() + extraSize);
	long ioLength = hddCloudlet.getCloudletIOLength();
	hddCloudlet.setCloudletLength(cpuLength);
	hddCloudlet.setCloudletIOLength(ioLength);

	Double cpuEst = hddCloudlet.getCloudletLength() == 0 ? null :
		hddCloudlet.getCloudletLength() / getCPUCapacity(getCurrentMipsShare());
	Double ioEst = hddCloudlet.getCloudletIOLength() == 0 ? null :
		hddCloudlet.getCloudletIOLength() / getIOCapacity(getCurrentIOMipsShare(), rcl);

	return refMin(cpuEst, ioEst);
    }

    /*
     * (non-Javadoc)
     * 
     * @see cloudsim.CloudletScheduler#cloudletSubmit(cloudsim.Cloudlet)
     */
    @Override
    public double cloudletSubmit(final Cloudlet cloudlet) {
	return cloudletSubmit(cloudlet, 0.0);
    }

    /**
     * Gets the status of a cloudlet.
     * 
     * @param cloudletId
     *            ID of the cloudlet
     * @return status of the cloudlet, -1 if cloudlet not found
     * @pre $none
     * @post $none
     */
    @Override
    public int getCloudletStatus(final int cloudletId) {
	int position = ResCloudletList.getPositionById(getCloudletExecList(), cloudletId);
	if (position >= 0) {
	    return getCloudletExecList().get(position).getCloudletStatus();
	}
	position = ResCloudletList.getPositionById(getCloudletExecList(), cloudletId);
	if (position >= 0) {
	    return getCloudletPausedList().get(position).getCloudletStatus();
	}
	return -1;
    }

    /**
     * Get utilization created by all cloudlets.
     * 
     * @param time
     *            the time
     * @return total utilization
     */
    @Override
    public double getTotalUtilizationOfCpu(final double time) {
	double totalUtilization = 0;
	for (HddResCloudlet gl : getCloudletExecList()) {
	    totalUtilization += gl.getCloudlet().getUtilizationOfCpu(time);
	}
	return totalUtilization;
    }

    /**
     * Informs about completion of some cloudlet in the VM managed by this
     * scheduler.
     * 
     * @return $true if there is at least one finished cloudlet; $false
     *         otherwise
     * @pre $none
     * @post $none
     */
    @Override
    public boolean isFinishedCloudlets() {
	return getCloudletFinishedList().size() > 0;
    }

    /**
     * Returns the next cloudlet in the finished list, $null if this list is
     * empty.
     * 
     * @return a finished cloudlet
     * @pre $none
     * @post $none
     */
    @Override
    public WebCloudlet getNextFinishedCloudlet() {
	if (getCloudletFinishedList().size() > 0) {
	    return getCloudletFinishedList().remove(0).getCloudlet();
	}
	return null;
    }

    /**
     * Returns the number of cloudlets runnning in the virtual machine.
     * 
     * @return number of cloudlets runnning
     * @pre $none
     * @post $none
     */
    @Override
    public int runningCloudlets() {
	return getCloudletExecList().size();
    }

    /**
     * Returns one cloudlet to migrate to another vm.
     * 
     * @return one running cloudlet
     * @pre $none
     * @post $none
     */
    @Override
    public Cloudlet migrateCloudlet() {
	HddResCloudlet rgl = getCloudletExecList().remove(0);
	rgl.finalizeCloudlet();
	return rgl.getCloudlet();
    }

    /**
     * Gets the cloudlet exec list.
     * 
     * @param <T>
     *            the generic type
     * @return the cloudlet exec list
     */
    @SuppressWarnings("unchecked")
    public <T extends HddResCloudlet> List<T> getCloudletExecList() {
	return (List<T>) cloudletExecList;
    }

    /**
     * Sets the cloudlet exec list.
     * 
     * @param <T>
     *            the generic type
     * @param cloudletExecList
     *            the new cloudlet exec list
     */
    protected <T extends HddResCloudlet> void setCloudletExecList(final List<T> cloudletExecList) {
	this.cloudletExecList = cloudletExecList;
    }

    /**
     * Gets the cloudlet paused list.
     * 
     * @param <T>
     *            the generic type
     * @return the cloudlet paused list
     */
    @SuppressWarnings("unchecked")
    protected <T extends HddResCloudlet> List<T> getCloudletPausedList() {
	return (List<T>) cloudletPausedList;
    }

    /**
     * Sets the cloudlet paused list.
     * 
     * @param <T>
     *            the generic type
     * @param cloudletPausedList
     *            the new cloudlet paused list
     */
    protected <T extends HddResCloudlet> void setCloudletPausedList(final List<T> cloudletPausedList) {
	this.cloudletPausedList = cloudletPausedList;
    }

    /**
     * Gets the cloudlet finished list.
     * 
     * @param <T>
     *            the generic type
     * @return the cloudlet finished list
     */
    protected List<? extends HddResCloudlet> getCloudletFinishedList() {
	return cloudletFinishedList;
    }

    /**
     * Sets the cloudlet finished list.
     * 
     * @param <T>
     *            the generic type
     * @param cloudletFinishedList
     *            the new cloudlet finished list
     */
    protected <T extends HddResCloudlet> void setCloudletFinishedList(final List<T> cloudletFinishedList) {
	this.cloudletFinishedList = cloudletFinishedList;
    }

    public List<? extends HddResCloudlet> getCloudletFailedList() {
	return cloudletFailedList;
    }

    public void setCloudletFailedList(final List<? extends HddResCloudlet> cloudletFailedList) {
	this.cloudletFailedList = cloudletFailedList;
    }

    /*
     * (non-Javadoc)
     * 
     * @see cloudsim.CloudletScheduler#getCurrentRequestedMips()
     */
    @Override
    public List<Double> getCurrentRequestedMips() {
	List<Double> mipsShare = new ArrayList<Double>();
	return mipsShare;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * cloudsim.CloudletScheduler#getTotalCurrentAvailableMipsForCloudlet(cloudsim
     * .ResCloudlet, java.util.List)
     */
    @Override
    public double getTotalCurrentAvailableMipsForCloudlet(final ResCloudlet rcl, final List<Double> mipsShare) {
	return getCPUCapacity(getCurrentMipsShare());
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * cloudsim.CloudletScheduler#getTotalCurrentAllocatedMipsForCloudlet(cloudsim
     * .ResCloudlet, double)
     */
    @Override
    public double getTotalCurrentAllocatedMipsForCloudlet(final ResCloudlet rcl, final double time) {
	return 0.0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * cloudsim.CloudletScheduler#getTotalCurrentRequestedMipsForCloudlet(cloudsim
     * .ResCloudlet, double)
     */
    @Override
    public double getTotalCurrentRequestedMipsForCloudlet(final ResCloudlet rcl, final double time) {
	// TODO Auto-generated method stub
	return 0.0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.cloudbus.cloudsim.CloudletScheduler#getCurrentRequestedUtilizationOfRam
     * ()
     */
    @Override
    public double getCurrentRequestedUtilizationOfRam() {
	double ram = 0;
	for (ResCloudlet cloudlet : cloudletExecList) {
	    ram += cloudlet.getCloudlet().getUtilizationOfRam(CloudSim.clock());
	}
	return ram;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.cloudbus.cloudsim.CloudletScheduler#getCurrentRequestedUtilizationOfBw
     * ()
     */
    @Override
    public double getCurrentRequestedUtilizationOfBw() {
	double bw = 0;
	for (ResCloudlet cloudlet : cloudletExecList) {
	    bw += cloudlet.getCloudlet().getUtilizationOfBw(CloudSim.clock());
	}
	return bw;
    }

    public List<Double> getCurrentRequestedIOMips() {
	List<Double> ioMipsShare = new ArrayList<>();
	return ioMipsShare;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void failAllCloudlets() {
	for (ListIterator<HddResCloudlet> iter = getCloudletExecList().listIterator(); iter.hasNext();) {
	    HddResCloudlet hddResCloudlet = iter.next();
	    iter.remove();
	    hddResCloudlet.setCloudletStatus(Cloudlet.FAILED);
	    ((List) cloudletFailedList).add(hddResCloudlet);
	}

	for (ListIterator<HddResCloudlet> iter = getCloudletPausedList().listIterator(); iter.hasNext();) {
	    HddResCloudlet hddResCloudlet = iter.next();
	    iter.remove();
	    hddResCloudlet.setCloudletStatus(Cloudlet.FAILED);
	    ((List) cloudletFailedList).add(hddResCloudlet);
	}
    }

}
