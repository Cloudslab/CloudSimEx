package org.cloudbus.cloudsim.ex.vm;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.cloudbus.cloudsim.CloudletScheduler;

/**
 * A type of virtual machine, which keeps track of its performance.
 * 
 * @author nikolay.grozev
 * 
 */
public class MonitorableVMex extends VMex {

    final private Map<Double, double[]> performanceObservations = new LinkedHashMap<>();
    private double summaryLength;

    public MonitorableVMex(int id, int userId, double mips, int numberOfPes, int ram, long bw, long size, String vmm,
	    CloudletScheduler cloudletScheduler) {
	super(id, userId, mips, numberOfPes, ram, bw, size, vmm, cloudletScheduler);
    }

    public MonitorableVMex(int id, int userId, double mips, int numberOfPes, int ram, long bw, long size, String vmm,
	    CloudletScheduler cloudletScheduler, VMMetadata metadata) {
	super(id, userId, mips, numberOfPes, ram, bw, size, vmm, cloudletScheduler, metadata);
    }

    public void updatePerformance(final double cpuUtil, final double ramUtil, final double diskUtil) {
	double currTime = getCurrentTime();
	performanceObservations.put(currTime, new double[] { cpuUtil, ramUtil, diskUtil });

	cleanupOldData(currTime);
    }

    private void cleanupOldData(double currTime) {
	List<Double> toRemove = new ArrayList<>();
	for (Map.Entry<Double, double[]> entry : performanceObservations.entrySet()) {
	    if (entry.getKey() < currTime - summaryLength) {
		toRemove.add(entry.getKey());
	    }
	}
	for (Double time : toRemove) {
	    performanceObservations.remove(time);
	}
    }

}
