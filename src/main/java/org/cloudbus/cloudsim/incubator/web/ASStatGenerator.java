package org.cloudbus.cloudsim.incubator.web;

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
	int cpuLen = Math.max(0, seqGenerators.get(CLOUDLET_LENGTH).nextValue().intValue());
	int ram = Math.max(0, seqGenerators.get(CLOUDLET_RAM).nextValue().intValue());

	return new WebCloudlet(idealStartTime, cpuLen, ram, webBroker);
    }
}
