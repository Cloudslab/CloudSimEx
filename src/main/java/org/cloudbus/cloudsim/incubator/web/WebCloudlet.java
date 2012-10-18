package org.cloudbus.cloudsim.incubator.web;

import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.UtilizationModelNull;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.incubator.util.Id;
import org.cloudbus.cloudsim.incubator.util.TextUtil;
import org.cloudbus.cloudsim.incubator.util.Textualize;
import org.cloudbus.cloudsim.lists.VmList;

/**
 * A web cloudlet is a cloudlet, which is a part of a web session. Usually it is
 * small in terms of RAM and CPU. Each web cloudlet is contained within a web
 * session.
 * 
 * <br/>
 * 
 * A distinct property of a web cloudlet is that it has ideal start time. This
 * ideal start time can be met... but can also be postponed if the previous web
 * cloudlets in the web session are delayed. Thus there is a difference between
 * the ideal start time and the actual start time.
 * 
 * @author nikolay.grozev
 * 
 */
@Textualize(properties = { "CloudletId", "SessionId", "Ram", "VmId", "Delay", "IdealStartTime", "ExecStartTime",
	"CloudletLength",
	"ActualCPUTime", "FinishTime", "CloudletStatusString", "Finished" })
public class WebCloudlet extends Cloudlet {

    private double idealStartTime;
    private int ram;
    private IWebBroker webBroker;
    private int sessionId;

    private int numberOfHddPes = 1;
    private long cloudletIOLength;

    /**
     * Constructs a new cloudlet.
     * 
     * @param idealStartTime
     *            - the ideal start time.
     * @param cloudletLength
     *            - number of processor instructions (MIPS) required.
     * @param ram
     *            - amount of ram in megabytes.
     * @param webBroker
     *            - the broker used to submit this cloudlet to the data center.
     * @param userId
     *            - the user id, as specified by the parent class.
     * @param record
     *            - the record flag, as specified by the parent class.
     */
    public WebCloudlet(final double idealStartTime,
	    final long cloudletLength,
	    final long cloudletIOLength,
	    final int ram,
	    final IWebBroker webBroker,
	    final boolean record) {

	super(Id.pollId(Cloudlet.class), cloudletLength, 1, 0, 0,
		new UtilizationModelFull(), null, new UtilizationModelNull(),
		record);
	this.idealStartTime = idealStartTime;
	this.ram = ram;
	this.webBroker = webBroker;
	this.cloudletIOLength = cloudletIOLength;
	setUserId(webBroker.getId());
	setUtilizationModelRam(new FixedUtilizationModel());
    }

    /**
     * Constructs a new cloudlet.
     * 
     * @param idealStartTime
     *            - the ideal start time.
     * @param cloudletLength
     *            - number of processor instructions (MIPS) required
     * @param ram
     *            - amount of ram in megabytes.
     * @param webBroker
     *            - the broker used to submit this cloudlet to the data center.
     */
    public WebCloudlet(final double idealStartTime,
	    final long cloudletLength,
	    final long cloudletIOLength,
	    final int ram,
	    final IWebBroker webBroker) {
	this(idealStartTime, cloudletLength, cloudletIOLength, ram, webBroker, false);
    }

    /**
     * Sets the userid. Does not allow user ids to be reset.
     * 
     * @see org.cloudbus.cloudsim.Cloudlet#setUserId(int)
     */
    @Override
    public void setUserId(int id) {
	if (id != webBroker.getId()) {
	    throw new IllegalArgumentException("Can not reset user id.");
	}
	super.setUserId(id);
    }

    /**
     * Returns the ideal start time of this web cloudlet.
     * 
     * @return the ideal start time of this web cloudlet.
     */
    public double getIdealStartTime() {
	return idealStartTime;
    }

    /**
     * Returns the id of the session this cloudlet belongs to.
     * 
     * @return the id of the session this cloudlet belongs to.
     */
    public int getSessionId() {
	return sessionId;
    }

    /**
     * Sets the session id of the session this cloudlet belongs to.
     * 
     * @param sessionId
     *            - the new session id.
     */
    public void setSessionId(final int sessionId) {
	this.sessionId = sessionId;
    }

    /**
     * Returns the delay in the start time. This is equal to actual_start_time -
     * ideal_start_time. In case some of them is unknown -1 is return.
     * 
     * @return the delay in the start time, as described above.
     */
    public double getDelay() {
	double delay = getExecStartTime() - getIdealStartTime();
	return delay < 0 ? -1 : delay;
    }

    /**
     * Returns the amount of ram memory used in megabytes.
     * 
     * @return the amount of ram memory used in megabytes.
     */
    public int getRam() {
	return ram;
    }

    /**
     * Returns the number of Harddisks this host has.
     * @return the number of Harddisks this host has.
     */
    public int getNumberOfHddPes() {
	return numberOfHddPes;
    }

    /**
     * Sets the number of Harddisks this host has.
     * @param numberOfHddPes - the number of Harddisks this host has. Must not be negative or 0.
     */
    public void setNumberOfHddPes(int numberOfHddPes) {
	this.numberOfHddPes = numberOfHddPes;
    }


    /**
     * Returns the total length of this cloudlet in terms of IO operations.
     * @return the total length of this cloudlet in terms of IO operations.
     */
    public long getCloudletTotalIOLength() {
	return getCloudletIOLength() * getNumberOfHddPes();
    }

    public long getCloudletIOLength() {
	return cloudletIOLength;
    }

    /**
     * Sets the total length of this cloudlet in terms of IO operations.
     * @param cloudletIOLength - total length of this cloudlet in terms of IO operations. Must be a positive number.
     */
    public void setCloudletIOLength(long cloudletIOLength) {
	this.cloudletIOLength = cloudletIOLength;
    }
    
    @Override
    public String toString() {
	return TextUtil.getTxtLine(this, "\t", true);
    }

    private class FixedUtilizationModel implements UtilizationModel {
	public double getUtilization(double time) {
	    double result = 0;
	    if (webBroker != null) {
		List<Vm> vms = webBroker.getVmList();
		Vm vm = VmList.getById(vms, getVmId());
		result = vm.getRam() / (double) ram;
	    }
	    return result;
	}
    }

}
