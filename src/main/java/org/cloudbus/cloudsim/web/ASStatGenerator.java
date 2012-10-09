package org.cloudbus.cloudsim.web;

import java.util.Map;

import org.uncommons.maths.number.NumberGenerator;

public class ASStatGenerator extends BaseStatGenerator<WebCloudlet> {

	private final WebBroker webBroker;

	public ASStatGenerator(WebBroker webBroker,
			Map<String, NumberGenerator<Double>> randomGenerators) {
		super(randomGenerators);
		this.webBroker = webBroker;
	}

	@Override
	protected WebCloudlet create(final double idealStartTime) {
		int cpuLen = seqGenerators.get(CLOUDLET_LENGTH).nextValue().intValue();
		int ram = seqGenerators.get(CLOUDLET_RAM).nextValue().intValue();
		return new WebCloudlet(idealStartTime, cpuLen, ram, webBroker);
	}
}
