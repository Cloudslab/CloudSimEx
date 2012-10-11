package org.cloudbus.cloudsim.incubator.web;

import java.util.Map;

import org.uncommons.maths.number.NumberGenerator;

/**
 * A statistical generator for Application Server cloudlets.
 * 
 * @author nikolay.grozev
 *
 */
public class ASStatGenerator extends BaseStatGenerator<WebCloudlet> {

    private final WebBroker webBroker;

    /**
     * Creates a new instance.
     * @param webBroker - the broker that will submit this CloudLet. Must not be null.
     * @param randomGenerators - the statistica random number generators as explained in the javadoc of the super class.
     */
    public ASStatGenerator(final WebBroker webBroker,
	    Map<String, NumberGenerator<Double>> randomGenerators) {
	super(randomGenerators);
	this.webBroker = webBroker;
    }

    /**
     * (non-Javadoc)
     * @see org.cloudbus.cloudsim.incubator.web.BaseStatGenerator#create(double)
     */
    @Override
    protected WebCloudlet create(final double idealStartTime) {
	int cpuLen = Math.max(0, seqGenerators.get(CLOUDLET_LENGTH).nextValue().intValue());
	int ram = Math.max(0, seqGenerators.get(CLOUDLET_RAM).nextValue().intValue());

	return new WebCloudlet(idealStartTime, cpuLen, ram, webBroker);
    }
}
