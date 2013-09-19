package org.cloudbus.cloudsim.ex.vm;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.cloudbus.cloudsim.CloudletScheduler;

/**
 * A type of virtual machine, which keeps track of its performance. This VM
 * relies on an external entity (e.g. a broker) to notify it of its observed
 * utilisation. The VM keeps track of the notifications and then defines its
 * utilisation for a given time as the mean of the observations for the last
 * <strong>summaryLength</strong> seconds. The <strong>summaryLength</strong>
 * parameter is specified in the constructors.
 * 
 * @author nikolay.grozev
 * 
 */
public class MonitoredVMex extends VMex {

    private final double summaryPeriodLength;

    /** The list/ordered set of all observations. */
    final private Map<Double, double[]> performanceObservations = new LinkedHashMap<>();
    /**
     * Keeping the sums of all observations, to avoid excessive looping over the
     * observations.
     */
    private double[] perfSums = new double[] { 0, 0, 0 };
    /**
     * The number/count of all observations. We keep it in a variable to avoid
     * looping.
     */
    private int perfCount = 0;

    private double[] lastUtilMeasurement = new double[] { 0, 0, 0 };
    private boolean newPerfDataAvailableFlag = false;

    /**
     * Constr.
     * 
     * @param name
     *            - a short readable description of the VM - e.g. DB-server.
     * @param userId
     * @param mips
     * @param numberOfPes
     * @param ram
     * @param bw
     * @param size
     * @param vmm
     * @param cloudletScheduler
     * @param summaryPeriodLength
     *            - the historical period, used to determine the utilisation at
     *            runtime.
     */
    public MonitoredVMex(final String name, final int userId, final double mips, final int numberOfPes, int ram,
	    final long bw, final long size, final String vmm, final CloudletScheduler cloudletScheduler,
	    final double summaryPeriodLength) {
	super(name, userId, mips, numberOfPes, ram, bw, size, vmm, cloudletScheduler);
	this.summaryPeriodLength = summaryPeriodLength;
    }

    /**
     * Constr.
     * 
     * @param name
     *            - a short readable description of the VM - e.g. DB-server.
     * @param userId
     * @param mips
     * @param numberOfPes
     * @param ram
     * @param bw
     * @param size
     * @param vmm
     * @param cloudletScheduler
     * @param metadata
     * @param summaryPeriodLength
     *            - the historical period, used to determine the utilisation at
     *            runtime.
     */
    public MonitoredVMex(final String name, final int userId, final double mips, final int numberOfPes, final int ram,
	    final long bw, final long size, final String vmm, final CloudletScheduler cloudletScheduler,
	    final VMMetadata metadata, final double summaryPeriodLength) {
	super(name, userId, mips, numberOfPes, ram, bw, size, vmm, cloudletScheduler, metadata);
	this.summaryPeriodLength = summaryPeriodLength;
    }

    protected double getSummaryPeriodLength() {
	return summaryPeriodLength;
    }

    /**
     * Notifies this VM of its utilisation.
     * 
     * @param cpuUtil
     *            - the CPU utilisation. Must be in the range [0,1];
     * @param ramUtil
     *            - the RAM utilisation. Must be in the range [0,1];
     * @param diskUtil
     *            - the Disk utilisation. Must be in the range [0,1];
     */
    public void updatePerformance(final double cpuUtil, final double ramUtil, final double diskUtil) {
	updatePerformance(new double[] { cpuUtil, ramUtil, diskUtil });
    }

    /**
     * Notifies this VM of its utilisation.
     * 
     * @param util
     *            - in the form [cpuUtil, ramUtil, diskUtil].
     */
    public void updatePerformance(final double[] util) {
	if (summaryPeriodLength >= 0) {
	    double currTime = getCurrentTime();

	    if (!newPerfDataAvailableFlag && Arrays.equals(util, this.lastUtilMeasurement)) {
		this.newPerfDataAvailableFlag = false;
	    } else {
		this.newPerfDataAvailableFlag = true;
	    }

	    performanceObservations.put(currTime, util);
	    for (int i = 0; i < util.length; i++) {
		perfSums[i] += util[i];
	    }
	    this.perfCount++;

	    cleanupOldData(currTime);
	}
    }

    /**
     * Returns the current CPU utilisation as a number in the range [0,1].
     * 
     * @return the current CPU utilisation as a number in the range [0,1].
     */
    public double getCPUUtil() {
	return getAveragedPerformance(getCurrentTime())[0];
    }

    /**
     * Returns the current RAM utilisation as a number in the range [0,1].
     * 
     * @return the current RAM utilisation as a number in the range [0,1].
     */
    public double getRAMUtil() {
	return getAveragedPerformance(getCurrentTime())[1];
    }

    /**
     * Returns the current Disk utilisation as a number in the range [0,1].
     * 
     * @return the current Disk utilisation as a number in the range [0,1].
     */
    public double getDiskUtil() {
	return getAveragedPerformance(getCurrentTime())[2];
    }

    /**
     * Returns the current utilisation as a array of numbers in the range [0,1]
     * in the from [cp_util, ram_util, disk_util]. <strong>NOTE</strong> calling
     * methods should not modify the resulting array, as a shallow copy may be
     * returned!
     * 
     * @return the current utilisation as a array of numbers in the range [0,1]
     *         in the from [cp_util, ram_util, disk_util].
     */
    public double[] getAveragedUtil() {
	return getAveragedPerformance(getCurrentTime());
    }

    private double[] getAveragedPerformance(final double currTime) {
	// If there has not been any update - return the cached value
	if (!newPerfDataAvailableFlag) {
	    return this.lastUtilMeasurement;
	} else {
	    cleanupOldData(currTime);
	    double[] result = new double[] { 0, 0, 0 };
	    if (summaryPeriodLength >= 0 && perfCount > 0) {
		for (int i = 0; i < result.length; i++) {
		    result[i] = perfSums[i] / perfCount;
		}
	    }

	    // Cache the value for further usage
	    newPerfDataAvailableFlag = false;
	    lastUtilMeasurement = result;

	    return result;
	}
    }

    private void cleanupOldData(final double currTime) {
	for (Iterator<Map.Entry<Double, double[]>> it = performanceObservations.entrySet().iterator(); it.hasNext();) {
	    Map.Entry<Double, double[]> entry = it.next();

	    if (entry.getKey() < currTime - summaryPeriodLength) {
		for (int i = 0; i < entry.getValue().length; i++) {
		    perfSums[i] -= entry.getValue()[i];
		}
		perfCount--;
		it.remove();
	    } else {
		break;
	    }
	}
    }

    @Override
    public MonitoredVMex clone(final CloudletScheduler scheduler) {
	if (!getClass().equals(MonitoredVMex.class)) {
	    throw new IllegalStateException("The operation is undefined for subclass: " + getClass().getCanonicalName());
	}

	MonitoredVMex result = new MonitoredVMex(getName(), getUserId(), getMips(), getNumberOfPes(), getRam(),
		getBw(), getSize(), getVmm(), scheduler, getMetadata().clone(), getSummaryPeriodLength());
	return result;
    }

    /**
     * For testing/debugging purposes only!
     * 
     * @return
     */
    public Map<Double, double[]> getPerformanceObservations() {
	return performanceObservations;
    }

}
