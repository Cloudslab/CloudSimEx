package org.cloudbus.cloudsim.ex.mapreduce;

import java.util.List;

import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.VmSchedulerSpaceShared;

public class VmSchedulerSpaceSharedMapReduce extends VmSchedulerSpaceShared
{

    public VmSchedulerSpaceSharedMapReduce(List<? extends Pe> pelist)
    {
	super(pelist);
	// TODO Auto-generated constructor stub
    }

    public List<Pe> getFreePes() {
	return super.getFreePes();
    }
}
