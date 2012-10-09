package org.cloudbus.cloudsim.web;

import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.UtilizationModelNull;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.lists.VmList;
import org.cloudbus.cloudsimgoodies.util.Id;

public class WebCloudlet extends Cloudlet {

	private double idealStartTime;
	private int ram;
	private WebBroker webBroker;

	public WebCloudlet(double idealStartTime,
			long cloudletLength, int ram, WebBroker webBroker, int userId, boolean record) {
		super(Id.pollId(Cloudlet.class), cloudletLength, 1, 0, 0, new UtilizationModelFull(),
				null, new UtilizationModelNull(), record);
		this.idealStartTime = idealStartTime;
		this.ram = ram;
		this.webBroker = webBroker;
		setUserId(webBroker.getId());
		setUtilizationModelRam(new FixedUtilizationModel());
	}

	public WebCloudlet(double idealStartTime,
			long cloudletLength, int ram, WebBroker webBroker) {
		super(Id.pollId(Cloudlet.class), cloudletLength, 1, 0, 0, new UtilizationModelFull(),
				null, new UtilizationModelNull());
		this.idealStartTime = idealStartTime;
		this.ram = ram;
		this.webBroker = webBroker;
		setUserId(webBroker.getId());
		setUtilizationModelRam(new FixedUtilizationModel());
	}

	@Override
	public void setUserId(int id) {
		if(id != webBroker.getId()) {
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

}
