package org.cloudbus.cloudsim.incubator.web;

import java.util.Map;

import org.uncommons.maths.number.NumberGenerator;

/**
 * A statistical generator for Server cloudlets.
 * 
 * @author nikolay.grozev
 * 
 */
public class StatGenerator extends BaseStatGenerator<WebCloudlet> {

    /**
     * Creates a new instance.
     * 
     * @param randomGenerators
     *            - the statistical random number generators as explained in the
     *            javadoc of the super class.
     */
    public StatGenerator(final Map<String, NumberGenerator<Double>> randomGenerators) {
	super(randomGenerators);
    }

    /**
     * Creates a new instance.
     * 
     * @param randomGenerators
     *            - the statistical random number generators as explained in the
     *            javadoc of the super class.
     * @param startTime
     *            - see the javadoc of the super class.
     * @param startTime
     *            - see the javadoc of the super class..
     */
    public StatGenerator(final Map<String, NumberGenerator<Double>> seqGenerators,
	    final double startTime, final double endTime) {
	super(seqGenerators, startTime, endTime);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.cloudbus.cloudsim.incubator.web.BaseStatGenerator#create(double)
     */
    @Override
    protected WebCloudlet create(final double idealStartTime) {
	long cpuLen = generateValue(CLOUDLET_LENGTH).longValue();
	int ram = generateValue(CLOUDLET_RAM).intValue();
	int ioLen = generateValue(CLOUDLET_IO).intValue();

	return new WebCloudlet(idealStartTime, cpuLen, ioLen, ram, -1);
    }

}
