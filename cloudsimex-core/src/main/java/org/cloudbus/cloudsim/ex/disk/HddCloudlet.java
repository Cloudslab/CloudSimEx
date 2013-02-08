package org.cloudbus.cloudsim.ex.disk;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.ex.util.Id;
import org.cloudbus.cloudsim.ex.util.TextUtil;

/**
 * A cloudlet that requires disk operations in addition to CPU ones. Besides
 * disk operations this cloudlet also defines RAM, which is constantly requred
 * while it is running.
 * 
 * @author nikolay.grozev
 * 
 */
public class HddCloudlet extends Cloudlet {

    private int numberOfHddPes = 1;
    protected long cloudletIOLength;
    protected final int ram;
    protected final DataItem data;

    /**
     * Constr.
     * 
     * @param cloudletLength
     * @param cloudletIOLength
     * @param pesNumber
     * @param numberOfHddPes
     * @param ram
     * @param userId
     * @param data
     * @param record
     */
    public HddCloudlet(long cloudletLength,
	    final long cloudletIOLength,
	    int pesNumber,
	    int numberOfHddPes,
	    final int ram,
	    final int userId,
	    final DataItem data,
	    final boolean record) {

	super(Id.pollId(Cloudlet.class), cloudletLength, pesNumber, 0, 0, new UtilizationModelFull(),
		new UtilizationModelFull(), new UtilizationModelFull(), record);
	this.ram = ram;
	this.cloudletIOLength = cloudletIOLength;
	this.data = data;
	this.numberOfHddPes = numberOfHddPes;
	setUserId(userId);
    }

    /**
     * Constr.
     * 
     * @param cloudletLength
     * @param cloudletIOLength
     * @param ram
     * @param userId
     * @param data
     * @param record
     */
    public HddCloudlet(final long cloudletLength,
	    final long cloudletIOLength,
	    final int ram,
	    final int userId,
	    final DataItem data,
	    final boolean record) {
	this(cloudletLength, cloudletIOLength, 1, 1, ram, userId, data, record);
    }

    /**
     * Constr.
     * 
     * @param cloudletLength
     * @param cloudletIOLength
     * @param ram
     * @param userId
     * @param data
     */
    public HddCloudlet(final long cloudletLength,
	    final long cloudletIOLength,
	    final int ram,
	    final int userId,
	    final DataItem data) {
	this(cloudletLength, cloudletIOLength, 1, 1, ram, userId, data, false);
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
     * 
     * @return the number of Harddisks this host has.
     */
    public int getNumberOfHddPes() {
	return numberOfHddPes;
    }

    /**
     * Sets the number of Harddisks this host has.
     * 
     * @param numberOfHddPes
     *            - the number of Harddisks this host has. Must not be negative
     *            or 0.
     */
    public void setNumberOfHddPes(int numberOfHddPes) {
	this.numberOfHddPes = numberOfHddPes;
    }

    /**
     * Returns the total length of this cloudlet in terms of IO operations.
     * 
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
     * 
     * @param cloudletIOLength
     *            - total length of this cloudlet in terms of IO operations.
     *            Must be a positive number.
     */
    public void setCloudletIOLength(long cloudletIOLength) {
	this.cloudletIOLength = cloudletIOLength;
    }

    /**
     * Returns the data used by this cloudlet.
     * 
     * @return - the data used by this cloudlet.
     */
    public DataItem getData() {
	return data;
    }

    @Override
    public String toString() {
	return TextUtil.getTxtLine(this, "\t", true);
    }

}