package org.cloudbus.cloudsim.ex.mapreduce;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerSpaceShared;
import org.cloudbus.cloudsim.ResCloudlet;
import org.cloudbus.cloudsim.ex.mapreduce.models.request.Task;

public class MapReduceCloudletScheduler extends CloudletSchedulerSpaceShared {

    // Override this function to calculate the fileTransferTime for each
    // cloudlet
    // not just for the first cloudlets on each vm
    @Override
    public double cloudletSubmit(Cloudlet cloudlet) {
	// calculate the expected time for cloudlet completion
	// double capacity = 0.0;
	int cpus = 0;
	for (Double mips : getCurrentMipsShare()) {
	    // capacity += mips;
	    if (mips > 0) {
		cpus++;
	    }
	}

	currentCpus = cpus;
	// capacity /= cpus;

	// use the current capacity to estimate the extra amount of
	// time to file transferring. It must be added to the cloudlet length
	// long executionTime = (long)((Task)
	// cloudlet).getTotalTimeInMillionInstructions();
	// cloudlet.setCloudletLength(executionTime);

	// it can go to the exec list
	if ((currentCpus - usedPes) >= cloudlet.getNumberOfPes()) {
	    ResCloudlet rcl = new ResCloudlet(cloudlet);
	    rcl.setCloudletStatus(Cloudlet.INEXEC);
	    for (int i = 0; i < cloudlet.getNumberOfPes(); i++) {
		rcl.setMachineAndPeId(0, i);
	    }
	    getCloudletExecList().add(rcl);
	    usedPes += cloudlet.getNumberOfPes();
	} else {// no enough free PEs: go to the waiting queue
	    ResCloudlet rcl = new ResCloudlet(cloudlet);
	    rcl.setCloudletStatus(Cloudlet.QUEUED);
	    getCloudletWaitingList().add(rcl);
	    return 0.0;
	}

	return ((Task) cloudlet).getTaskExecutionTimeInSeconds();
    }
}
