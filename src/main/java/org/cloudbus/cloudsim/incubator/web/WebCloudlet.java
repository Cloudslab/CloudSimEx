package org.cloudbus.cloudsim.incubator.web;

import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.UtilizationModelNull;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.incubator.util.Id;
import org.cloudbus.cloudsim.lists.VmList;

public class WebCloudlet extends Cloudlet {

    private double idealStartTime;
    private int ram;
    private WebBroker webBroker;
    private int sessionId;

    public WebCloudlet(double idealStartTime, long cloudletLength, int ram,
	    WebBroker webBroker, int userId, boolean record) {
	super(Id.pollId(Cloudlet.class), cloudletLength, 1, 0, 0,
		new UtilizationModelFull(), null, new UtilizationModelNull(),
		record);
	this.idealStartTime = idealStartTime;
	this.ram = ram;
	this.webBroker = webBroker;
	setUserId(webBroker.getId());
	setUtilizationModelRam(new FixedUtilizationModel());
    }

    public WebCloudlet(double idealStartTime, long cloudletLength, int ram,
	    WebBroker webBroker) {
	super(Id.pollId(Cloudlet.class), cloudletLength, 1, 0, 0,
		new UtilizationModelFull(), null, new UtilizationModelNull());
	this.idealStartTime = idealStartTime;
	this.ram = ram;
	this.webBroker = webBroker;
	setUserId(webBroker.getId());
	setUtilizationModelRam(new FixedUtilizationModel());
    }

    @Override
    public void setUserId(int id) {
	if (id != webBroker.getId()) {
	    throw new IllegalArgumentException("Can not reset user id.");
	}
	super.setUserId(id);
    }

    public double getIdealStartTime() {
	return idealStartTime;
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
    
    public int getSessionId() {
	return sessionId;
    }
    
    public void setSessionId(int sessionId) {
	this.sessionId = sessionId;
    }

    @Override
    public String toString() {
	double delay = getExecStartTime() - getIdealStartTime(); 
	return String
		.format("Id=%d \t SessId=%d \t VmId=%d \t Delay=%s \t Duration=%3.3f \t IdealStart=%3.3f \t Start=%3.3f \t End=%3.3f \t Stat=%s \t MIPs=%d \t RAM=%d",
			getCloudletId(),
			getSessionId(),
			getVmId(),
			delay < 0? "N/A" : String.format("%3.3f", delay),
			getActualCPUTime(),
			getIdealStartTime(),
			getExecStartTime(),
			getFinishTime(),
			String.valueOf(getCloudletStatusString()),
			getCloudletLength(),
			ram);
    }

}
