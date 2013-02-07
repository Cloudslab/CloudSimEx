package org.cloudbus.cloudsim.incubator.web;

import java.util.Map;

import org.cloudbus.cloudsim.incubator.disk.DataItem;
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
     * @param data
     *            - the data used by the generator, or null if no data is used.
     */
    public StatGenerator(final Map<String, NumberGenerator<Double>> randomGenerators, final DataItem data) {
	super(randomGenerators, data);
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
     *            - see the javadoc of the super class.
     * @param data
     *            - the data used by the generator, or null if no data is used.
     */
    public StatGenerator(final Map<String, NumberGenerator<Double>> seqGenerators,
	    final double startTime, final double endTime, final DataItem data) {
	super(seqGenerators, startTime, endTime, data);
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

	return new WebCloudlet(idealStartTime, cpuLen, ioLen, ram, -1, getData());
    }

}
