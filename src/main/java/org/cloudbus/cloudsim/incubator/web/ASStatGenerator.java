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

    private final IWebBroker webBroker;

    /**
     * Creates a new instance.
     * 
     * @param webBroker
     *            - the broker that will submit this CloudLet. Must not be null.
     * @param randomGenerators
     *            - the statistical random number generators as explained in the
     *            javadoc of the super class.
     */
    public ASStatGenerator(final IWebBroker webBroker,
	    Map<String, NumberGenerator<Double>> randomGenerators) {
	super(randomGenerators);
	this.webBroker = webBroker;
    }

    /**
     * Creates a new instance.
     * 
     * @param webBroker
     *            - the broker that will submit this CloudLet. Must not be null.
     * @param randomGenerators
     *            - the statistical random number generators as explained in the
     *            javadoc of the super class.
     * @param startTime
     *            - see the javadoc of the super class.
     * @param startTime
     *            - see the javadoc of the super class..
     */
    public ASStatGenerator(final IWebBroker webBroker, final Map<String, NumberGenerator<Double>> seqGenerators,
	    final double startTime, final double endTime) {
	super(seqGenerators, startTime, endTime);
	this.webBroker = webBroker;
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.cloudbus.cloudsim.incubator.web.BaseStatGenerator#create(double)
     */
    @Override
    protected WebCloudlet create(final double idealStartTime) {
	long cpuLen = generateValue(CLOUDLET_LENGTH).longValue();
	int ram = generateValue(CLOUDLET_RAM).intValue();
	int ioLen = generateValue(CLOUDLET_LENGTH).intValue();

	return new WebCloudlet(idealStartTime, cpuLen, ioLen, ram, webBroker);
    }

}
