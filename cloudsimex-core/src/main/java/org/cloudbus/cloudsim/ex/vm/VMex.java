package org.cloudbus.cloudsim.ex.vm;

import org.cloudbus.cloudsim.CloudletScheduler;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;

/**
 * An extension of the base cloudsim VM.
 * 
 * @author nikolay.grozev
 * 
 */
public class VMex extends Vm {

    private VMStatus status;

    private double submissionTime = -1;
    private double startTime = -1;
    private double endTime = -1;

    public VMex(int id, int userId, double mips, int numberOfPes, int ram, long bw, long size, String vmm,
	    CloudletScheduler cloudletScheduler) {
	super(id, userId, mips, numberOfPes, ram, bw, size, vmm, cloudletScheduler);
    }

    // Unfortunately the super class already has a boolean property if the VM is
    // in init state, so we need to make sure the to properties are in synch.
    // That's why we override the set/get methods to make sure they are synched.
    // ...

    @Override
    public void setBeingInstantiated(final boolean beingInstantiated) {
	if (status != null && status != VMStatus.INITIALISING) {
	    throw new IllegalStateException("The initiated status can not be set if the VM is in " +
		    status.name() + " state.");
	}

	super.setBeingInstantiated(beingInstantiated);
	setStatus(super.isBeingInstantiated() ? VMStatus.INITIALISING : VMStatus.RUNNING);
    }

    @Override
    public boolean isBeingInstantiated() {
	if ((super.isBeingInstantiated() && status != null && status != VMStatus.INITIALISING)
		|| (!super.isBeingInstantiated() && status == VMStatus.INITIALISING)) {
	    throw new IllegalStateException("The initiated states are not in synch. state: " + status.name()
		    + " init flag:" + super.isBeingInstantiated());
	}
	return super.isBeingInstantiated();
    }

    /**
     * Returns the status of the VM.
     * 
     * @return the status of the VM.
     */
    public VMStatus getStatus() {
	return status;
    }

    /**
     * Sets the status of the VM, if possible. For example, if the VM is in
     * terminated state, and the init state is attempted to be set it will not
     * be possible to set the state. In case the state can not be set and
     * {@link IllegalStateException} is thrown. The logic about which state
     * transitions are possible is implemented in {@link VMStatus}.
     * 
     * @param status
     *            - the new status to set. Must not be null.
     */
    public void setStatus(final VMStatus status) {
	switch (status) {
	    case INITIALISING:
		setSubmissionTime(CloudSim.clock());
		break;
	    case RUNNING:
		setStartTime(CloudSim.clock());
		break;
	    case TERMINATED:
		setEndTime(CloudSim.clock());
		break;
	    default:
		throw new IllegalArgumentException("Unknown status " + status.name());
	}

	this.status = status;
	super.setBeingInstantiated(VMStatus.INITIALISING == status);
    }

    /**
     * Sets the submission time (time of creation before booting) of the VM.
     * 
     * @return the submission time.
     */
    public double getSubmissionTime() {
	return submissionTime;
    }

    /**
     * Sets the submission time (time of creation before booting) of the VM.
     * 
     * @param submissionTime
     *            - the new submission time.
     */
    private void setSubmissionTime(double submissionTime) {
	this.submissionTime = submissionTime;
    }

    /**
     * Returns the starting time (after it has booted) of this VM.
     * 
     * @return - the starting time (after it has booted) of this VM.
     */
    public double getStartTime() {
	return startTime;
    }

    /**
     * Sets the starting time (after it has booted) of this VM.
     * 
     * @param startTime
     *            - the starting time (after it has booted) of this VM. Must be
     *            after the submission time.
     */
    private void setStartTime(double startTime) {
	this.startTime = startTime;
    }

    /**
     * Returns the ending time (due to failure or termination) of this VM. If
     * the VM is still running then -1 is returned.
     * 
     * @return the ending time (due to failure or termination) of this VM.
     */
    public double getEndTime() {
	return endTime;
    }

    /**
     * Sets the ending time (due to failure or termination) of this VM.
     * 
     * @param endTime
     *            - the end time. Must be after the starting time.
     */
    private void setEndTime(double endTime) {
	this.endTime = endTime;
    }

    
    /**
     * Returns the duration for which this VM has been functional (after booting).
     * @return the duration for which this VM has been functional (after booting).
     */
    public double getTimeAfterBooting() {
	double endTime = getEndTime() < 0 ? CloudSim.clock() : getEndTime();
	return endTime - startTime;
    }
    
    /**
     * Returns the duration for which this VM has existed (after its creation).
     * @return the duration for which this VM has existed (after its creation).
     */
    public double getTimeAfterSubmission() {
	double endTime = getEndTime() < 0 ? CloudSim.clock() : getEndTime();
	return endTime - submissionTime;
    }
}
