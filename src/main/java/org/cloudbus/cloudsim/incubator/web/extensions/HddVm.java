package org.cloudbus.cloudsim.incubator.web.extensions;

import java.util.Arrays;
import java.util.List;

import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.incubator.util.Id;

/**
 * A virtual machine with a harddisk. Unlike the other CloudSim implementaions
 * of virtual machines, this one models the contention caused by the I/O
 * opearations of the cloudlets.
 * 
 * @author nikolay.grozev
 * 
 */
public class HddVm extends Vm {

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
     */
    public HddVm(int userId, double mips, int numberOfPes, int ram, long bw, long size, String vmm,
	    HddCloudletSchedulerTimeShared cloudletScheduler) {
	super(Id.pollId(HddVm.class), userId, mips, numberOfPes, ram, bw, size, vmm, cloudletScheduler);
    }

    /*
     * (non-Javadoc)
     * @see org.cloudbus.cloudsim.Vm#updateVmProcessing(double, java.util.List)
     */
    @Override
    @Deprecated
    public double updateVmProcessing(double currentTime, List<Double> mipsShare) {
	return updateVmProcessing(currentTime, mipsShare, Arrays.<Double> asList());
    }

    /**
     * Updates the processing of the VM
     *  
     * @param currentTime - current simulation time.
     * @param mipsShare - array with MIPS share of each Pe available to the scheduler.
     * @param iopsShare - array with I/O MIPS share of each Harddisk available to the scheduler.
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
     * @see org.cloudbus.cloudsim.Vm#getCloudletScheduler()
     */
    @Override
    public HddCloudletSchedulerTimeShared getCloudletScheduler() {
	return (HddCloudletSchedulerTimeShared) super.getCloudletScheduler();
    }

}
