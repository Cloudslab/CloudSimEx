package org.cloudbus.cloudsim.incubator.web.extensions;

import java.util.List;

import org.cloudbus.cloudsim.CloudletScheduler;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.incubator.util.Id;

public class HDVm extends Vm {

    public HDVm(int userId, double mips, int numberOfPes, int ram, long bw, long size, String vmm,
	    HddCloudletScheduler cloudletScheduler) {
	super(Id.pollId(HDVm.class), userId, mips, numberOfPes, ram, bw, size, vmm, cloudletScheduler);
    }

    @Override
    public double updateVmProcessing(double currentTime, List<Double> mipsShare) {
	throw new UnsupportedOperationException();
    }

    public double updateVmProcessing(double currentTime, List<Double> mipsShare, List<Double> iopsShare) {
	if (mipsShare != null && iopsShare != null) {
	    return getCloudletScheduler().updateVmProcessing(currentTime, mipsShare/*, iopsShare*/);
	}
	return 0.0;
    }

}
