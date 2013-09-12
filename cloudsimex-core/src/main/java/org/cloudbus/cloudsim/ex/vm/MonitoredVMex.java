package org.cloudbus.cloudsim.ex.vm;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
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

    final private Map<Double, double[]> performanceObservations = new LinkedHashMap<>();
    private final double summaryPeriodLength;

    private double lastUtilUpdateTime = -1;
    private double lastUtilMeasurmentTime = -1;
    private double[] lastUtilMeasurement = new double[] { 0, 0, 0 };

    /**
     * Constr.
     * 
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
    public MonitoredVMex(final int userId, final double mips, final int numberOfPes, int ram,
	    final long bw, final long size, final String vmm, final CloudletScheduler cloudletScheduler,
	    final double summaryPeriodLength) {
	super(userId, mips, numberOfPes, ram, bw, size, vmm, cloudletScheduler);
	this.summaryPeriodLength = summaryPeriodLength;
    }

    /**
     * 
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
    public MonitoredVMex(final int userId, final double mips, final int numberOfPes, final int ram,
	    final long bw, final long size, final String vmm, final CloudletScheduler cloudletScheduler,
	    final VMMetadata metadata, final double summaryPeriodLength) {
	super(userId, mips, numberOfPes, ram, bw, size, vmm, cloudletScheduler, metadata);
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
	if (summaryPeriodLength >= 0) {
	    double currTime = getCurrentTime();
	    performanceObservations.put(currTime, new double[] { cpuUtil, ramUtil, diskUtil });

	    cleanupOldData(currTime);
	}
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
	    
	    this.lastUtilUpdateTime = currTime;
	    performanceObservations.put(currTime, util);
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
	if (this.lastUtilUpdateTime < this.lastUtilMeasurmentTime) {
	    return this.lastUtilMeasurement;
	} else {
	    cleanupOldData(currTime);
	    double[] result = new double[] { 0, 0, 0 };
	    if (summaryPeriodLength >= 0 && !performanceObservations.isEmpty()) {
		for (Map.Entry<Double, double[]> entry : performanceObservations.entrySet()) {
		    for (int i = 0; i < result.length; i++) {
			result[i] += entry.getValue()[i];
		    }
		}
		for (int i = 0; i < result.length; i++) {
		    result[i] = result[i] / performanceObservations.size();
		}
	    }

	    // Cache the value for further usage
	    lastUtilMeasurmentTime = currTime;
	    lastUtilMeasurement = result;

	    return result;
	}
    }

    private void cleanupOldData(final double currTime) {
	List<Double> toRemove = new ArrayList<>();
	for (Map.Entry<Double, double[]> entry : performanceObservations.entrySet()) {
	    if (entry.getKey() < currTime - summaryPeriodLength) {
		toRemove.add(entry.getKey());
	    }
	}
	for (Double time : toRemove) {
	    performanceObservations.remove(time);
	}
    }

    @Override
    public MonitoredVMex clone(final CloudletScheduler scheduler) {
	if (!getClass().equals(MonitoredVMex.class)) {
	    throw new IllegalStateException("The operation is undefined for subclass: " + getClass().getCanonicalName());
	}

	MonitoredVMex result = new MonitoredVMex(getUserId(), getMips(), getNumberOfPes(), getRam(), getBw(),
		getSize(),
		getVmm(), scheduler, getMetadata().clone(), getSummaryPeriodLength());
	return result;
    }
}
